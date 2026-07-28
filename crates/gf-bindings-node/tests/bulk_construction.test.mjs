// Native acceptance for Node Arrow IPC bulk construction (#2551).
// Coverage markers for #2552 omission checks: empty, single-row, multi-row,
// mixed-property, identity/entity_uuid, endpoint, malformed-input, atomicity,
// retry-conflict/idempotency, receipt, reopen.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  Bool,
  Field,
  FixedSizeBinary,
  Int64,
  RecordBatchStreamWriter,
  Schema,
  Table,
  Utf8,
  tableFromIPC,
  vectorFromArray,
} from "apache-arrow";
import { GraphForge } from "../index.js";

const NODE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c101";
const EDGE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c102";
const EMPTY_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c100";
const SINGLE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c103";
const MISSING_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c104";

const bulkNodeSchema = new Schema(
  [
    new Field("node_uuid", new FixedSizeBinary(16), true),
    new Field("label", new Utf8(), false),
    // Property columns must be lexicographically ordered after topology fields.
    new Field("active", new Bool(), false),
    new Field("name", new Utf8(), false),
    new Field("score", new Int64(), false),
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
    new Field("since", new Int64(), false),
  ],
  new Map([
    ["graphforge.bulk_contract_version", "1"],
    ["graphforge.bulk_kind", "edge"],
    ["graphforge.row_order", "logical_input_order"],
  ]),
);

function tableToIpc(table) {
  return Buffer.from(
    RecordBatchStreamWriter.writeAll(table).toUint8Array(true),
  );
}

function nodeTable(names, scores = null) {
  if (names.length === 0) {
    return new Table(bulkNodeSchema);
  }
  const resolvedScores =
    scores ?? names.map((_, index) => BigInt((index + 1) * 10));
  return new Table(bulkNodeSchema, {
    node_uuid: vectorFromArray(
      names.map(() => null),
      new FixedSizeBinary(16),
    ),
    label: vectorFromArray(
      names.map(() => "Person"),
      new Utf8(),
    ),
    active: vectorFromArray(
      names.map((_, index) => index % 2 === 0),
      new Bool(),
    ),
    name: vectorFromArray(names, new Utf8()),
    score: vectorFromArray(resolvedScores, new Int64()),
  });
}

function projectDigest(root) {
  const hash = createHash("sha256");
  const walk = (path) => {
    for (const entry of readdirSync(path, { withFileTypes: true }).sort(
      (a, b) => a.name.localeCompare(b.name),
    )) {
      const child = join(path, entry.name);
      if (entry.isDirectory()) {
        walk(child);
        continue;
      }
      hash.update(child.slice(root.length));
      hash.update(readFileSync(child));
    }
  };
  walk(root);
  return hash.digest("hex");
}

test("publishBulkNodes/Edges accept canonical IPC and reopen identically", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-bulk-node-"));
  try {
    let forge = new GraphForge(project);

    const emptyReceipt = tableFromIPC(
      forge.publishBulkNodes(EMPTY_OPERATION, tableToIpc(nodeTable([]))),
    );
    assert.equal(emptyReceipt.numRows, 0);

    const singleReceipt = tableFromIPC(
      forge.publishBulkNodes(SINGLE_OPERATION, tableToIpc(nodeTable(["Solo"]))),
    );
    assert.equal(singleReceipt.numRows, 1);

    const published = nodeTable(["Alice", "Bob"], [BigInt(10), BigInt(20)]);
    const nodeReceipt = tableFromIPC(
      forge.publishBulkNodes(NODE_OPERATION, tableToIpc(published)),
    );
    assert.equal(nodeReceipt.numRows, 2);
    const nodeIds = [...nodeReceipt.getChild("entity_uuid")];

    const before = projectDigest(project);
    const retry = tableFromIPC(
      forge.publishBulkNodes(NODE_OPERATION, tableToIpc(published)),
    );
    assert.deepEqual([...retry.getChild("entity_uuid")], nodeIds);
    assert.equal(projectDigest(project), before);

    let conflicted = false;
    try {
      forge.publishBulkNodes(NODE_OPERATION, tableToIpc(nodeTable(["Carol"])));
    } catch (error) {
      conflicted =
        error?.code === "GF_IDEMPOTENCY_CONFLICT" ||
        String(error?.message ?? error).includes("IDEMPOTENCY") ||
        String(error?.message ?? error)
          .toLowerCase()
          .includes("conflict");
    }
    assert.equal(conflicted, true);
    assert.equal(projectDigest(project), before);

    let missingEndpoint = false;
    try {
      const missing = Buffer.from("018f0f4e7b8c7000800000000000dead", "hex");
      const badEdge = new Table(bulkEdgeSchema, {
        edge_uuid: vectorFromArray([null], new FixedSizeBinary(16)),
        rel_type: vectorFromArray(["KNOWS"], new Utf8()),
        source_uuid: vectorFromArray([nodeIds[0]], new FixedSizeBinary(16)),
        target_uuid: vectorFromArray([missing], new FixedSizeBinary(16)),
        since: vectorFromArray([BigInt(2020)], new Int64()),
      });
      forge.publishBulkEdges(MISSING_OPERATION, tableToIpc(badEdge));
    } catch (error) {
      missingEndpoint =
        error?.code === "GF_VALIDATION" ||
        String(error?.message ?? error).includes("missing_endpoint") ||
        String(error?.message ?? error).includes("GF_BULK_VALIDATION");
    }
    assert.equal(missingEndpoint, true);
    assert.equal(projectDigest(project), before);

    const edgeTable = new Table(bulkEdgeSchema, {
      edge_uuid: vectorFromArray([null], new FixedSizeBinary(16)),
      rel_type: vectorFromArray(["KNOWS"], new Utf8()),
      source_uuid: vectorFromArray([nodeIds[0]], new FixedSizeBinary(16)),
      target_uuid: vectorFromArray([nodeIds[1]], new FixedSizeBinary(16)),
      since: vectorFromArray([BigInt(2020)], new Int64()),
    });
    const edgeReceipt = tableFromIPC(
      forge.publishBulkEdges(EDGE_OPERATION, tableToIpc(edgeTable)),
    );
    assert.equal(edgeReceipt.numRows, 1);
    assert.deepEqual([...edgeReceipt.getChild("rel_type")], ["KNOWS"]);

    forge = new GraphForge(project);
    const reopened = tableFromIPC(
      forge.execute(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b, a.score AS score, r.since AS since ORDER BY a, b",
      ),
    );
    assert.deepEqual([...reopened.getChild("a")], ["Alice"]);
    assert.deepEqual([...reopened.getChild("b")], ["Bob"]);
    assert.deepEqual(
      [...reopened.getChild("score")].map((value) => Number(value)),
      [10],
    );
    assert.deepEqual(
      [...reopened.getChild("since")].map((value) => Number(value)),
      [2020],
    );

    let malformed = false;
    try {
      forge.publishBulkNodes(EDGE_OPERATION, Buffer.from("not-ipc"));
    } catch {
      malformed = true;
    }
    assert.equal(malformed, true);
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
