"""Import-session begin/checkpoint/resume/abort lifecycle through Python."""

from __future__ import annotations

from pathlib import Path
import tempfile
import uuid

import pyarrow as pa


def check_import_session_lifecycle() -> None:
    import graphforge as gf

    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp) / "project"
        project.mkdir()
        forge = gf.GraphForge(str(project))
        operation = str(uuid.uuid4())
        session = forge.begin_import_session(operation_uuid=operation)
        status = session.status()
        assert status["phase"] == "open"
        node_uuid = uuid.uuid4().bytes
        table = pa.table(
            {
                "node_uuid": pa.array([node_uuid], type=pa.binary(16)),
                "label": pa.array(["Person"]),
            }
        )
        session.append_arrow("node", table)
        progress = session.checkpoint()
        assert progress["files_pending"] >= 1
        session_uuid = session.session_uuid
        del session
        resumed = forge.resume_import_session(session_uuid)
        assert resumed.session_uuid == session_uuid
        aborted = resumed.abort()
        assert aborted["files_accepted"] >= 1
        cleaned = forge.cleanup_stale_import_sessions(max_age_secs=0)
        assert cleaned == 0


def check_import_session_narrow_integer_reads_back() -> None:
    """A narrow Arrow integer column imports, commits and reads back as Int64."""
    import graphforge as gf

    with tempfile.TemporaryDirectory() as tmp:
        project = Path(tmp) / "project"
        project.mkdir()
        forge = gf.GraphForge(str(project))
        session = forge.begin_import_session(operation_uuid=str(uuid.uuid4()))
        schema = pa.schema(
            [
                pa.field("node_uuid", pa.binary(16), nullable=True),
                pa.field("label", pa.utf8(), nullable=False),
                pa.field("age", pa.int32(), nullable=True),
            ],
            metadata={
                b"graphforge.bulk_contract_version": b"1",
                b"graphforge.bulk_kind": b"node",
                b"graphforge.row_order": b"logical_input_order",
            },
        )
        node_uuids = [
            uuid.UUID(f"018f0f4e-7b8c-7000-8000-0000000d000{index}").bytes for index in range(3)
        ]
        table = pa.Table.from_arrays(
            [
                pa.array(node_uuids, type=pa.binary(16)),
                pa.array(["Person"] * 3),
                pa.array([-2147483648, None, 2147483647], type=pa.int32()),
            ],
            schema=schema,
        )
        session.append_arrow("node", table)
        session.validate()
        session.commit()
        forge.close()

        reopened = gf.GraphForge(str(project))
        rows = reopened.execute("MATCH (n:Person) RETURN n.age AS age ORDER BY age")
        assert rows.schema.field("age").type == pa.int64(), rows.schema
        assert rows.column("age").to_pylist() == [-2147483648, 2147483647, None]
        matched = reopened.execute("MATCH (n:Person) WHERE n.age = 2147483647 RETURN count(n) AS c")
        assert matched.column("c").to_pylist() == [1]
        reopened.close()


def main() -> None:
    check_import_session_lifecycle()
    check_import_session_narrow_integer_reads_back()


if __name__ == "__main__":
    main()
