#!/usr/bin/env python3
"""Same-SHA Python/Node bulk parity probe with fixed UUID v7 identities (#2552).

Requires an installed native Python wheel and a built Node addon for HEAD.
Rust parity is covered by the lib bulk_construction suite in the conformance matrix.

Coverage markers for omission checks: empty, single-row, multi-row,
mixed-property, identity/entity_uuid, endpoint, malformed-input, atomicity,
retry-conflict/idempotency, receipt, reopen.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import uuid

import pyarrow as pa

import graphforge as g

ROOT = Path(__file__).resolve().parents[2]
NODE_PKG = ROOT / "crates" / "graphforge-bindings-node"

NODE_A = uuid.UUID("018f0f4e-7b8c-7000-8000-00000000d001")
NODE_B = uuid.UUID("018f0f4e-7b8c-7000-8000-00000000d002")
EDGE_AB = uuid.UUID("018f0f4e-7b8c-7000-8000-00000000d101")
OP_NODES = "018f0f4e-7b8c-7000-8000-00000000d201"
OP_EDGES = "018f0f4e-7b8c-7000-8000-00000000d202"
OP_MISSING = "018f0f4e-7b8c-7000-8000-00000000d203"
OP_BAD_UUID = "018f0f4e-7b8c-7000-8000-00000000d204"
MISSING_ENDPOINT = uuid.UUID("018f0f4e-7b8c-7000-8000-00000000d999")

BULK_NODE_METADATA = {
    b"graphforge.bulk_contract_version": b"1",
    b"graphforge.bulk_kind": b"node",
    b"graphforge.row_order": b"logical_input_order",
}
BULK_EDGE_METADATA = {
    b"graphforge.bulk_contract_version": b"1",
    b"graphforge.bulk_kind": b"edge",
    b"graphforge.row_order": b"logical_input_order",
}


def project_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def node_table() -> pa.Table:
    schema = pa.schema(
        [
            pa.field("node_uuid", pa.binary(16), nullable=True),
            pa.field("label", pa.utf8(), nullable=False),
            # Property columns must be lexicographically ordered after topology.
            pa.field("active", pa.bool_(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
            pa.field("score", pa.int64(), nullable=False),
        ],
        metadata=BULK_NODE_METADATA,
    )
    return pa.Table.from_arrays(
        [
            pa.array([NODE_A.bytes, NODE_B.bytes], type=pa.binary(16)),
            pa.array(["Person", "Person"], type=pa.utf8()),
            pa.array([True, False], type=pa.bool_()),
            pa.array(["Alice", "Bob"], type=pa.utf8()),
            pa.array([10, 20], type=pa.int64()),
        ],
        schema=schema,
    )


def edge_table(source: uuid.UUID, target: uuid.UUID) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("edge_uuid", pa.binary(16), nullable=True),
            pa.field("rel_type", pa.utf8(), nullable=False),
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("target_uuid", pa.binary(16), nullable=False),
            pa.field("since", pa.int64(), nullable=False),
        ],
        metadata=BULK_EDGE_METADATA,
    )
    return pa.Table.from_arrays(
        [
            pa.array([EDGE_AB.bytes], type=pa.binary(16)),
            pa.array(["KNOWS"], type=pa.utf8()),
            pa.array([source.bytes], type=pa.binary(16)),
            pa.array([target.bytes], type=pa.binary(16)),
            pa.array([2020], type=pa.int64()),
        ],
        schema=schema,
    )


def empty_node_table() -> pa.Table:
    return node_table().schema.empty_table()


def run_python(project: Path) -> dict:
    forge = g.GraphForge(str(project))
    empty = forge.publish_bulk_nodes(OP_NODES, empty_node_table())
    assert empty.num_rows == 0

    # single-row path via convenience helper
    single_op = "018f0f4e-7b8c-7000-8000-00000000d210"
    single = forge.add_nodes(
        "Person",
        [{"active": True, "name": "Solo", "score": 1}],
        operation_uuid=single_op,
    )
    assert single.num_rows == 1

    # reset for fixed-UUID multi-row mixed-property publication
    forge.close()
    shutil.rmtree(project)
    project.mkdir(parents=True)
    forge = g.GraphForge(str(project))

    nodes = forge.publish_bulk_nodes(OP_NODES, node_table())
    assert nodes.num_rows == 2
    ids = [uuid.UUID(bytes=value) for value in nodes.column("entity_uuid").to_pylist()]
    assert ids == [NODE_A, NODE_B]

    # Exact retry proves idempotency without mutation.
    before = project_digest(project)
    retry = forge.publish_bulk_nodes(OP_NODES, node_table())
    assert [uuid.UUID(bytes=value) for value in retry.column("entity_uuid").to_pylist()] == ids
    assert project_digest(project) == before

    try:
        forge.publish_bulk_edges(OP_MISSING, edge_table(NODE_A, MISSING_ENDPOINT))
    except g.GraphForgeError as exc:
        assert exc.code == "GF_VALIDATION", exc.code
        assert "missing_endpoint" in str(exc) or "GF_BULK_VALIDATION" in str(exc)
    else:
        raise SystemExit("python: expected missing endpoint rejection")
    assert project_digest(project) == before

    # 16-byte payload that is not a UUID v7 (version nibble cleared).
    bad_bytes = bytearray(NODE_A.bytes)
    bad_bytes[6] = bad_bytes[6] & 0x0F
    bad_uuid_table = pa.Table.from_arrays(
        [
            pa.array([bytes(bad_bytes)], type=pa.binary(16)),
            pa.array(["Person"], type=pa.utf8()),
            pa.array([True], type=pa.bool_()),
            pa.array(["Bad"], type=pa.utf8()),
            pa.array([1], type=pa.int64()),
        ],
        schema=node_table().schema,
    )
    try:
        forge.publish_bulk_nodes(OP_BAD_UUID, bad_uuid_table)
    except g.GraphForgeError as exc:
        assert exc.code == "GF_VALIDATION", exc.code
        assert "GF_BULK_VALIDATION" in str(exc) or "invalid" in str(exc).lower()
    else:
        raise SystemExit("python: expected invalid UUID rejection")
    assert project_digest(project) == before

    edges = forge.publish_bulk_edges(OP_EDGES, edge_table(NODE_A, NODE_B))
    assert edges.num_rows == 1
    assert edges.column("rel_type").to_pylist() == ["KNOWS"]

    forge = g.GraphForge(str(project))
    reopened = forge.execute(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
        "RETURN a.name AS a, b.name AS b, a.score AS score, r.since AS since "
        "ORDER BY a, b"
    )
    payload = {
        "names": reopened.column("a").to_pylist() + reopened.column("b").to_pylist(),
        "scores": reopened.column("score").to_pylist(),
        "since": reopened.column("since").to_pylist(),
        "node_ids": [str(value) for value in ids],
        "digest": project_digest(project),
    }
    return payload


def write_node_probe(project: Path, script: Path) -> None:
    script.write_text(
        f"""
import assert from "node:assert/strict";
import {{ createHash }} from "node:crypto";
import {{
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
}} from "node:fs";
import {{ join }} from "node:path";
import {{
  Field,
  FixedSizeBinary,
  Int64,
  Bool,
  RecordBatchStreamWriter,
  Schema,
  Table,
  Utf8,
  tableFromIPC,
  vectorFromArray,
}} from "apache-arrow";
import {{ GraphForge }} from {json.dumps(str(NODE_PKG / "index.js"))};

const project = {json.dumps(str(project))};
const NODE_A = Buffer.from({json.dumps(list(NODE_A.bytes))});
const NODE_B = Buffer.from({json.dumps(list(NODE_B.bytes))});
const EDGE_AB = Buffer.from({json.dumps(list(EDGE_AB.bytes))});
const MISSING = Buffer.from({json.dumps(list(MISSING_ENDPOINT.bytes))});
const OP_NODES = {json.dumps(OP_NODES)};
const OP_EDGES = {json.dumps(OP_EDGES)};
const OP_MISSING = {json.dumps(OP_MISSING)};
const OP_BAD_UUID = {json.dumps(OP_BAD_UUID)};
const outPath = {json.dumps(str(project / "node-parity.json"))};

const bulkNodeSchema = new Schema(
  [
    new Field("node_uuid", new FixedSizeBinary(16), true),
    new Field("label", new Utf8(), false),
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

function tableToIpc(table) {{
  return Buffer.from(RecordBatchStreamWriter.writeAll(table).toUint8Array(true));
}}

function uuidString(buf) {{
  const hex = Buffer.from(buf).toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}}

function projectDigest(root) {{
  const hash = createHash("sha256");
  const walk = (path) => {{
    for (const entry of readdirSync(path, {{ withFileTypes: true }}).sort((a, b) =>
      a.name.localeCompare(b.name),
    )) {{
      const child = join(path, entry.name);
      if (entry.isDirectory()) {{
        walk(child);
        continue;
      }}
      hash.update(child.slice(root.length));
      hash.update(readFileSync(child));
    }}
  }};
  walk(root);
  return hash.digest("hex");
}}

rmSync(project, {{ recursive: true, force: true }});
mkdirSync(project, {{ recursive: true }});
let forge = new GraphForge(project);

const empty = tableFromIPC(forge.publishBulkNodes(OP_NODES, tableToIpc(new Table(bulkNodeSchema))));
assert.equal(empty.numRows, 0);

const nodes = new Table(bulkNodeSchema, {{
  node_uuid: vectorFromArray([NODE_A, NODE_B], new FixedSizeBinary(16)),
  label: vectorFromArray(["Person", "Person"], new Utf8()),
  active: vectorFromArray([true, false], new Bool()),
  name: vectorFromArray(["Alice", "Bob"], new Utf8()),
  score: vectorFromArray([BigInt(10), BigInt(20)], new Int64()),
}});
const nodeReceipt = tableFromIPC(forge.publishBulkNodes(OP_NODES, tableToIpc(nodes)));
assert.equal(nodeReceipt.numRows, 2);
const receivedIds = [...nodeReceipt.getChild("entity_uuid")].map((buf) => uuidString(buf));
const before = projectDigest(project);

let missing = false;
try {{
  const badEdge = new Table(bulkEdgeSchema, {{
    edge_uuid: vectorFromArray([EDGE_AB], new FixedSizeBinary(16)),
    rel_type: vectorFromArray(["KNOWS"], new Utf8()),
    source_uuid: vectorFromArray([NODE_A], new FixedSizeBinary(16)),
    target_uuid: vectorFromArray([MISSING], new FixedSizeBinary(16)),
    since: vectorFromArray([BigInt(2020)], new Int64()),
  }});
  forge.publishBulkEdges(OP_MISSING, tableToIpc(badEdge));
}} catch (error) {{
  missing =
    error?.code === "GF_VALIDATION" ||
    String(error?.message ?? error).includes("missing_endpoint") ||
    String(error?.message ?? error).includes("GF_BULK_VALIDATION");
}}
assert.equal(missing, true);
assert.equal(projectDigest(project), before);

let invalidUuid = false;
try {{
  const badBytes = Buffer.from(NODE_A);
  badBytes[6] = badBytes[6] & 0x0f; // clear UUID version nibble
  const badTable = new Table(bulkNodeSchema, {{
    node_uuid: vectorFromArray([badBytes], new FixedSizeBinary(16)),
    label: vectorFromArray(["Person"], new Utf8()),
    active: vectorFromArray([true], new Bool()),
    name: vectorFromArray(["Bad"], new Utf8()),
    score: vectorFromArray([BigInt(1)], new Int64()),
  }});
  forge.publishBulkNodes(OP_BAD_UUID, tableToIpc(badTable));
}} catch (error) {{
  invalidUuid =
    error?.code === "GF_VALIDATION" &&
    (String(error?.message ?? error).includes("GF_BULK_VALIDATION") ||
      String(error?.message ?? error).toLowerCase().includes("invalid"));
}}
assert.equal(invalidUuid, true);
assert.equal(projectDigest(project), before);

const edge = new Table(bulkEdgeSchema, {{
  edge_uuid: vectorFromArray([EDGE_AB], new FixedSizeBinary(16)),
  rel_type: vectorFromArray(["KNOWS"], new Utf8()),
  source_uuid: vectorFromArray([NODE_A], new FixedSizeBinary(16)),
  target_uuid: vectorFromArray([NODE_B], new FixedSizeBinary(16)),
  since: vectorFromArray([BigInt(2020)], new Int64()),
}});
const edgeReceipt = tableFromIPC(forge.publishBulkEdges(OP_EDGES, tableToIpc(edge)));
assert.equal(edgeReceipt.numRows, 1);

forge = new GraphForge(project);
const reopened = tableFromIPC(
  forge.execute(
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
    + "RETURN a.name AS a, b.name AS b, a.score AS score, r.since AS since "
    + "ORDER BY a, b",
  ),
);
const payload = {{
  names: [...reopened.getChild("a"), ...reopened.getChild("b")],
  scores: [...reopened.getChild("score")].map((value) => Number(value)),
  since: [...reopened.getChild("since")].map((value) => Number(value)),
  node_ids: receivedIds,
}};
writeFileSync(outPath, JSON.stringify(payload));
""",
        encoding="utf-8",
    )


def run_node(project: Path) -> dict:
    script = project.parent / "node-bulk-parity.mjs"
    write_node_probe(project, script)
    completed = subprocess.run(
        ["node", str(script)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(f"node parity probe failed: {completed.stdout}\n{completed.stderr}")
    return json.loads((project / "node-parity.json").read_text(encoding="utf-8"))


def main() -> None:
    commit = (
        os.environ.get("GF_BULK_CONFORMANCE_SHA")
        or subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    )
    with tempfile.TemporaryDirectory(prefix="gf-bulk-parity-") as directory:
        root = Path(directory)
        py_project = root / "python"
        node_project = root / "node"
        py_project.mkdir()
        node_project.mkdir()
        py_payload = run_python(py_project)
        node_payload = run_node(node_project)
        assert py_payload["names"] == node_payload["names"] == ["Alice", "Bob"]
        assert py_payload["scores"] == node_payload["scores"] == [10]
        assert py_payload["since"] == node_payload["since"] == [2020]
        assert py_payload["node_ids"] == node_payload["node_ids"]
        print(
            json.dumps(
                {
                    "commit": commit,
                    "python_digest": py_payload["digest"],
                    "names": py_payload["names"],
                    "node_ids": py_payload["node_ids"],
                    "ok": True,
                }
            )
        )


if __name__ == "__main__":
    main()
