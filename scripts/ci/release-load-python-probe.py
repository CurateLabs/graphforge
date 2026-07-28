#!/usr/bin/env python3
"""Exercise one load fixture through the installed native Python facade."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import tempfile

import pyarrow as pa

import graphforge

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
NODE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000b001"
EDGE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000b002"


def directory_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def fingerprint(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def load_fixture(forge: graphforge.GraphForge, fixture: dict) -> None:
    nodes = fixture["nodes"]
    # Match Node/Rust probes and bulk_node_input_schema: required columns are
    # non-null; only node_uuid (generated) and optional properties stay nullable.
    node_table = pa.table(
        {
            "node_uuid": pa.array([None] * len(nodes), type=pa.binary(16)),
            "label": [node["label"] for node in nodes],
            "active": [node["active"] for node in nodes],
            "group": pa.array([node["group"] for node in nodes], type=pa.int64()),
            "name": [node["name"] for node in nodes],
            "nullable": [node["nullable"] for node in nodes],
            "ordinal": pa.array([node["ordinal"] for node in nodes], type=pa.int64()),
            "salience": pa.array([node["salience"] for node in nodes], type=pa.float64()),
        },
        schema=pa.schema(
            [
                pa.field("node_uuid", pa.binary(16), nullable=True),
                pa.field("label", pa.utf8(), nullable=False),
                pa.field("active", pa.bool_(), nullable=False),
                pa.field("group", pa.int64(), nullable=False),
                pa.field("name", pa.utf8(), nullable=False),
                pa.field("nullable", pa.utf8(), nullable=True),
                pa.field("ordinal", pa.int64(), nullable=False),
                pa.field("salience", pa.float64(), nullable=False),
            ],
            metadata=BULK_NODE_METADATA,
        ),
    )
    receipt = forge.publish_bulk_nodes(NODE_OPERATION, node_table)
    node_ids = receipt.column("entity_uuid").to_pylist()
    if len(node_ids) != len(nodes):
        raise RuntimeError("bulk node receipt row count drifted from fixture")

    edges = fixture["edges"]
    edge_table = pa.table(
        {
            "edge_uuid": pa.array([None] * len(edges), type=pa.binary(16)),
            "rel_type": [edge["type"] for edge in edges],
            "source_uuid": pa.array(
                [node_ids[edge["source"]] for edge in edges], type=pa.binary(16)
            ),
            "target_uuid": pa.array(
                [node_ids[edge["target"]] for edge in edges], type=pa.binary(16)
            ),
            "weight": pa.array([edge["weight"] for edge in edges], type=pa.float64()),
        },
        schema=pa.schema(
            [
                pa.field("edge_uuid", pa.binary(16), nullable=True),
                pa.field("rel_type", pa.utf8(), nullable=False),
                pa.field("source_uuid", pa.binary(16), nullable=False),
                pa.field("target_uuid", pa.binary(16), nullable=False),
                pa.field("weight", pa.float64(), nullable=False),
            ],
            metadata=BULK_EDGE_METADATA,
        ),
    )
    forge.publish_bulk_edges(EDGE_OPERATION, edge_table)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    request = json.loads(args.request.read_text(encoding="utf-8"))
    fixture_path = Path(request["fixture"])
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    workload = request["workload"]["id"]
    persisted = 0
    with tempfile.TemporaryDirectory(prefix="gf-load-python-") as raw:
        project = Path(raw)
        forge = graphforge.GraphForge(str(project))
        load_fixture(forge, fixture)
        nodes = forge.execute("MATCH (n) RETURN n.name AS name ORDER BY name")
        node_rows = nodes.num_rows
        schema_sha256 = fingerprint(
            [
                [field.name, "utf8" if str(field.type) == "string" else str(field.type)]
                for field in nodes.schema
            ]
        )
        node_result_sha256 = fingerprint(nodes.column("name").to_pylist())
        edge_rows = forge.execute("MATCH ()-[r]->() RETURN r").num_rows
        rank_rows = 0
        find_rows = 0
        rank_result_sha256 = fingerprint([])
        find_result_sha256 = fingerprint([])
        if workload.startswith("m18-"):
            rank = forge.rank("Entity", by="degree", via="LINK")
            rank_rows = rank.num_rows
            rank_result_sha256 = fingerprint(
                sorted(
                    zip(
                        rank.column("name").to_pylist(),
                        rank.column("score").to_pylist(),
                        strict=True,
                    )
                )
            )
        if workload.startswith("m19-"):
            forge.index("Entity", properties=["name"])
            found = forge.find("n-00000001", label="Entity", limit=10)
            find_rows = found.num_rows
            find_result_sha256 = fingerprint(
                sorted(
                    zip(
                        found.column("name").to_pylist(),
                        found.column("matched_on").to_pylist(),
                        strict=True,
                    )
                )
            )
        forge.close()
        persisted = directory_bytes(project)
        reopened = graphforge.GraphForge(str(project))
        reopened_nodes = reopened.execute("MATCH (n) RETURN n.name AS name ORDER BY name")
        reopen_node_rows = reopened_nodes.num_rows
        reopen_node_result_sha256 = fingerprint(reopened_nodes.column("name").to_pylist())
        if workload.startswith("m18-"):
            if reopened.rank("Entity", by="degree", via="LINK").num_rows != rank_rows:
                raise RuntimeError("rank result changed after reopen")
        if workload.startswith("m19-"):
            if reopened.find("n-00000001", label="Entity", limit=10).num_rows != find_rows:
                raise RuntimeError("find result changed after reopen")
        temporary = max(0, directory_bytes(project) - persisted)
        reopened.close()
    report = {
        "schema": "graphforge-load-native-probe/1",
        "language": "python",
        "dataset_sha256": request["manifest"]["content_sha256"],
        "workload": workload,
        "observed": {
            "node_rows": node_rows,
            "edge_rows": edge_rows,
            "rank_rows": rank_rows,
            "find_rows": find_rows,
            "reopen_node_rows": reopen_node_rows,
            "schema_sha256": schema_sha256,
            "ordering_sha256": node_result_sha256,
            "node_result_sha256": node_result_sha256,
            "rank_result_sha256": rank_result_sha256,
            "find_result_sha256": find_result_sha256,
        },
        "persisted_bytes": persisted,
        "temporary_bytes": temporary,
        "cleanup": "complete",
        "reopen_equivalent": (
            reopen_node_rows == node_rows and reopen_node_result_sha256 == node_result_sha256
        ),
    }
    args.output.write_text(json.dumps(report, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
