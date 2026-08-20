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


def main() -> None:
    check_import_session_lifecycle()


if __name__ == "__main__":
    main()
