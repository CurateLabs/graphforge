// First-use ergonomics acceptance for the documented Node primary path (#1209).
//
// The documented quickstart flow needs no handwritten UUIDv7 generator, no
// positional `undefined` chains, and no manual Arrow IPC decoding. Bulk
// publication accepts an omitted operation id: Rust mints a UUIDv7, the
// receipt exposes it, an explicit replay of that id is idempotent, and
// conflicting reuse is still refused.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  Field,
  FixedSizeBinary,
  RecordBatchStreamWriter,
  Schema,
  Table,
  Utf8,
  tableFromIPC,
  vectorFromArray,
} from "apache-arrow";
import { GraphForge } from "../index.js";
import { query } from "../lib/query.mjs";

function formatUuid(value) {
  const hex = Buffer.from(value).toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
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

function scores(table) {
  const names = [...table.getChild("name").toArray()];
  const values = [...table.getChild("score").toArray()];
  return Object.fromEntries(names.map((name, index) => [name, values[index]]));
}

function nodeBulkTable(rows) {
  const schema = new Schema(
    [
      new Field("node_uuid", new FixedSizeBinary(16), true),
      new Field("label", new Utf8(), false),
      new Field("name", new Utf8(), false),
    ],
    new Map([
      ["graphforge.bulk_contract_version", "1"],
      ["graphforge.bulk_kind", "node"],
      ["graphforge.row_order", "logical_input_order"],
    ]),
  );
  return new Table(schema, {
    node_uuid: vectorFromArray(
      rows.map(() => null),
      new FixedSizeBinary(16),
    ),
    label: vectorFromArray(
      rows.map(() => "Person"),
      new Utf8(),
    ),
    name: vectorFromArray(rows, new Utf8()),
  });
}

function tableToIpc(table) {
  return Buffer.from(
    RecordBatchStreamWriter.writeAll(table).toUint8Array(true),
  );
}

test("the documented Node quickstart path needs no IPC plumbing", () => {
  const forge = new GraphForge();
  const survey = forge.addNode("Paper", { title: "Survey" });
  const methods = forge.addNode("Paper", { title: "Methods" });
  const replication = forge.addNode("Paper", { title: "Replication" });
  forge.addEdge(survey, "CITES", methods);
  forge.addEdge(survey, "CITES", replication);
  forge.addEdge(replication, "CITES", methods);

  const table = query(
    forge,
    "MATCH (:Paper)-[:CITES]->(paper:Paper) " +
      "RETURN paper.title AS title, count(*) AS citations " +
      "ORDER BY citations DESC, title",
  );
  assert.deepEqual(table.toArray().map((row) => row.toJSON()), [
    { title: "Methods", citations: 2n },
    { title: "Replication", citations: 1n },
  ]);

  forge.close();
});

test("rank and cluster accept the documented options-object forms", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (a)-[:LINK]->(b)",
  );

  // README rank form: damping + iterations, no undefined placeholders.
  const pagerank = forge.rank("Person", "pagerank", {
    via: "LINK",
    directed: true,
    damping: 0.5,
    iterations: 1,
  });
  assert.deepEqual(scores(tableFromIPC(pagerank)), { A: 0.375, B: 0.625 });

  // README rank form: clustering normalization control.
  const clustering = forge.rank("Person", "clustering_coefficient", {
    directed: true,
    clusteringNormalization: "neighbor_edges",
  });
  assert.equal(clustering.byteLength > 0, true);

  // README cluster form: synchronous rounds fed by an Int64 label property.
  forge.execute(
    "MATCH (n:Person) SET n.external_id = CASE n.name WHEN 'A' THEN 1 ELSE 2 END",
  );
  const clustered = forge.cluster("Person", "label_propagation", {
    directed: false,
    synchronousIterations: 0,
    initialLabelProperty: "external_id",
  });
  const clusterTable = tableFromIPC(clustered);
  assert.deepEqual(
    [...clusterTable.getChild("community_id").toArray()].sort(),
    [1n, 2n],
  );

  forge.close();
});

test("bulk publication mints and exposes an optional operation id", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-first-use-node-"));
  try {
    const forge = new GraphForge(project);

    const receipt = tableFromIPC(
      forge.publishBulkNodes(tableToIpc(nodeBulkTable(["Ada", "Grace"]))),
    );
    assert.equal(receipt.numRows, 2);
    const minted = formatUuid(receipt.getChild("operation_uuid").get(0));
    assert.match(
      minted,
      /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    const nodeIds = [...receipt.getChild("entity_uuid")];

    const before = projectDigest(project);

    // The minted identity passed back explicitly replays idempotently.
    const replay = tableFromIPC(
      forge.publishBulkNodes(
        tableToIpc(nodeBulkTable(["Ada", "Grace"])),
        minted,
      ),
    );
    assert.deepEqual([...replay.getChild("entity_uuid")], nodeIds);
    assert.equal(formatUuid(replay.getChild("operation_uuid").get(0)), minted);
    assert.equal(projectDigest(project), before);

    // The minted identity refuses a conflicting payload.
    let conflicted = false;
    try {
      forge.publishBulkNodes(tableToIpc(nodeBulkTable(["Knuth"])), minted);
    } catch (error) {
      conflicted =
        error?.code === "GF_IDEMPOTENCY_CONFLICT" ||
        String(error?.message ?? error).includes("IDEMPOTENCY") ||
        String(error?.message ?? error).toLowerCase().includes("conflict");
    }
    assert.equal(conflicted, true);
    assert.equal(projectDigest(project), before);

    forge.close();
  } finally {
    rmSync(project, { recursive: true, force: true });
  }
});
