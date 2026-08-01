"""Native-wheel acceptance for Python Arrow bulk construction (#2550).

Coverage markers for #2552 omission checks: empty, single-row, multi-row,
mixed-property, identity/entity_uuid, endpoint, malformed-input, atomicity,
retry-conflict/idempotency, receipt, reopen.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import tempfile

import pyarrow as pa

import graphforge as g

NODE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c001"
EDGE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c002"
RETRY_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c003"
SINGLE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c004"
MISSING_OPERATION = "018f0f4e-7b8c-7000-8000-00000000c006"

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

BULK_NODE_SCHEMA = pa.schema(
    [
        pa.field("node_uuid", pa.binary(16), nullable=True),
        pa.field("label", pa.utf8(), nullable=False),
        pa.field("name", pa.utf8(), nullable=False),
    ],
    metadata=BULK_NODE_METADATA,
)
BULK_MIXED_NODE_SCHEMA = pa.schema(
    [
        pa.field("node_uuid", pa.binary(16), nullable=True),
        pa.field("label", pa.utf8(), nullable=False),
        # Property columns must be lexicographically ordered after topology fields.
        pa.field("active", pa.bool_(), nullable=False),
        pa.field("name", pa.utf8(), nullable=False),
        pa.field("score", pa.int64(), nullable=False),
    ],
    metadata=BULK_NODE_METADATA,
)
BULK_EDGE_SCHEMA = pa.schema(
    [
        pa.field("edge_uuid", pa.binary(16), nullable=True),
        pa.field("rel_type", pa.utf8(), nullable=False),
        pa.field("source_uuid", pa.binary(16), nullable=False),
        pa.field("target_uuid", pa.binary(16), nullable=False),
        pa.field("since", pa.int64(), nullable=False),
    ],
    metadata=BULK_EDGE_METADATA,
)


def project_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def canonical_node_table(names: list[str]) -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([None] * len(names), type=pa.binary(16)),
            pa.array(["Person"] * len(names), type=pa.utf8()),
            pa.array(names, type=pa.utf8()),
        ],
        schema=BULK_NODE_SCHEMA,
    )


def check_bulk_construction(project: Path) -> None:
    forge = g.GraphForge(str(project))

    # Empty batch returns a zero-row receipt without mutating the project.
    empty = forge.publish_bulk_nodes(NODE_OPERATION, canonical_node_table([]))
    assert empty.num_rows == 0, empty
    assert forge.execute("MATCH (n) RETURN count(n) AS c").column("c").to_pylist() == [0]

    # Single-row convenience publication.
    single = forge.add_nodes(
        "Person",
        [{"name": "Solo"}],
        operation_uuid=SINGLE_OPERATION,
    )
    assert single.num_rows == 1, single

    # Mixed-property multi-row table is the canonical publication path.
    mixed = pa.Table.from_arrays(
        [
            pa.array([None, None], type=pa.binary(16)),
            pa.array(["Person", "Person"], type=pa.utf8()),
            pa.array([True, False], type=pa.bool_()),
            pa.array(["Alice", "Bob"], type=pa.utf8()),
            pa.array([10, 20], type=pa.int64()),
        ],
        schema=BULK_MIXED_NODE_SCHEMA,
    )
    table_receipt = forge.publish_bulk_nodes(NODE_OPERATION, mixed)
    assert table_receipt.num_rows == 2, table_receipt
    table_ids = table_receipt.column("entity_uuid").to_pylist()

    names = forge.execute(
        "MATCH (n:Person) WHERE n.name IN ['Alice', 'Bob'] "
        "RETURN n.name AS name, n.score AS score ORDER BY name"
    )
    assert names.column("name").to_pylist() == ["Alice", "Bob"]
    assert names.column("score").to_pylist() == [10, 20]

    # Exact retry is idempotent; conflicting reuse fails without mutation.
    before = project_digest(project)
    retry = forge.publish_bulk_nodes(NODE_OPERATION, mixed)
    assert retry.column("entity_uuid").to_pylist() == table_ids
    assert project_digest(project) == before

    # Convenience list[dict] path also converges when reusing the operation.
    list_receipt = forge.add_nodes(
        "Person",
        [
            {"active": True, "name": "Alice", "score": 10},
            {"active": False, "name": "Bob", "score": 20},
        ],
        operation_uuid=NODE_OPERATION,
    )
    assert list_receipt.column("entity_uuid").to_pylist() == table_ids
    assert project_digest(project) == before

    try:
        forge.publish_bulk_nodes(NODE_OPERATION, canonical_node_table(["Carol"]))
    except g.GraphForgeError as exc:
        assert exc.code == "GF_IDEMPOTENCY_CONFLICT", exc.code
    else:
        raise SystemExit("expected idempotency conflict for changed bulk reuse")

    assert project_digest(project) == before

    # Missing endpoint fails atomically before mutation.
    missing_target = bytes.fromhex("018f0f4e7b8c7000800000000000dead")
    bad_edge = pa.Table.from_arrays(
        [
            pa.array([None], type=pa.binary(16)),
            pa.array(["KNOWS"], type=pa.utf8()),
            pa.array([table_ids[0]], type=pa.binary(16)),
            pa.array([missing_target], type=pa.binary(16)),
            pa.array([2020], type=pa.int64()),
        ],
        schema=BULK_EDGE_SCHEMA,
    )
    try:
        forge.publish_bulk_edges(MISSING_OPERATION, bad_edge)
    except g.GraphForgeError as exc:
        assert exc.code == "GF_VALIDATION", exc.code
        assert "missing_endpoint" in str(exc) or "GF_BULK_VALIDATION" in str(exc)
    else:
        raise SystemExit("expected missing endpoint rejection")
    assert project_digest(project) == before

    # Edges accept convenience endpoint column names and reopen identically.
    edge_receipt = forge.add_edges(
        "KNOWS",
        [{"src_id": table_ids[0], "dst_id": table_ids[1], "since": 2020}],
        operation_uuid=EDGE_OPERATION,
    )
    assert edge_receipt.num_rows == 1, edge_receipt
    assert edge_receipt.column("rel_type").to_pylist() == ["KNOWS"]

    forge = g.GraphForge(str(project))
    reopened = forge.execute(
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
        "RETURN a.name AS a, b.name AS b, a.score AS score, r.since AS since "
        "ORDER BY a, b"
    )
    assert reopened.column("a").to_pylist() == ["Alice"]
    assert reopened.column("b").to_pylist() == ["Bob"]
    assert reopened.column("score").to_pylist() == [10]
    assert reopened.column("since").to_pylist() == [2020]

    # Malformed convenience input fails before mutation.
    before = project_digest(project)
    try:
        forge.add_nodes("Person", "not-a-container", operation_uuid=RETRY_OPERATION)
    except TypeError:
        pass
    else:
        raise SystemExit("expected TypeError for malformed bulk container")
    assert project_digest(project) == before


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="gf-bulk-py-") as directory:
        check_bulk_construction(Path(directory))
        print("bulk_construction: ok")
