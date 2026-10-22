"""First-use ergonomics acceptance for the documented Python primary path (#1209).

The documented quickstart flow needs no handwritten UUIDv7 generator, no
positional ``None`` placeholders, and no manual Arrow IPC decoding. Bulk
publication accepts an omitted operation id: Rust mints a UUIDv7, the receipt
exposes it, an explicit replay of that id is idempotent, and conflicting reuse
is still refused.
"""

from __future__ import annotations

import tempfile
import hashlib
import uuid
from pathlib import Path

import graphforge as g

QUICKSTART_QUERY = """
    MATCH (:Paper)-[:CITES]->(paper:Paper)
    RETURN paper.title AS title, count(*) AS citations
    ORDER BY citations DESC, title
"""
QUICKSTART_EXPECTED = [
    {"title": "Methods", "citations": 2},
    {"title": "Replication", "citations": 1},
]


def project_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def check_documented_quickstart() -> None:
    """Run docs/guide/quickstart.md exactly as documented and assert the result."""
    forge = g.GraphForge()
    survey = forge.add_node("Paper", title="Survey")
    methods = forge.add_node("Paper", title="Methods")
    replication = forge.add_node("Paper", title="Replication")
    forge.add_edge(survey, "CITES", methods)
    forge.add_edge(survey, "CITES", replication)
    forge.add_edge(replication, "CITES", methods)

    result = forge.execute(QUICKSTART_QUERY)
    assert result.to_pylist() == QUICKSTART_EXPECTED, result.to_pylist()

    forge.close()


def receipt_operation_uuid(receipt) -> str:
    value = receipt.column("operation_uuid")[0].as_py()
    operation = uuid.UUID(bytes=value)
    assert operation.version == 7, operation
    return str(operation)


def check_bulk_operation_id_is_optional(project: Path) -> None:
    """Omitted bulk id mints a Rust UUIDv7; explicit replay is idempotent."""
    forge = g.GraphForge(str(project))

    nodes = forge.add_nodes("Person", [{"name": "Ada"}, {"name": "Grace"}])
    assert nodes.num_rows == 2, nodes
    minted = receipt_operation_uuid(nodes)
    node_ids = nodes.column("entity_uuid").to_pylist()

    edges = forge.add_edges(
        "KNOWS",
        [{"src_id": node_ids[0], "dst_id": node_ids[1]}],
    )
    assert edges.num_rows == 1, edges
    receipt_operation_uuid(edges)

    before = project_digest(project)

    # The minted identity passed back explicitly replays idempotently.
    replay = forge.add_nodes(
        "Person",
        [{"name": "Ada"}, {"name": "Grace"}],
        operation_uuid=minted,
    )
    assert replay.column("entity_uuid").to_pylist() == node_ids
    assert receipt_operation_uuid(replay) == minted
    assert project_digest(project) == before

    # The minted identity refuses a conflicting payload.
    try:
        forge.add_nodes("Person", [{"name": "Knuth"}], operation_uuid=minted)
    except g.GraphForgeError as exc:
        assert exc.code == "GF_IDEMPOTENCY_CONFLICT", exc.code
    else:
        raise SystemExit("expected idempotency conflict for changed bulk reuse")
    assert project_digest(project) == before

    forge.close()


if __name__ == "__main__":
    check_documented_quickstart()
    with tempfile.TemporaryDirectory(prefix="gf-first-use-py-") as directory:
        check_bulk_operation_id_is_optional(Path(directory))
    print("first_use_ergonomics: ok")
