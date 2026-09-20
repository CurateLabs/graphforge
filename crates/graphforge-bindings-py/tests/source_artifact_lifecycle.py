"""Source and Artifact lifecycle binding parity (#1349)."""

from __future__ import annotations

import tempfile
import uuid

import pyarrow as pa

import graphforge
from graphforge.exceptions import StorageError


def _open_admitted(path: str) -> graphforge.GraphForge | None:
    try:
        return graphforge.GraphForge(path)
    except StorageError as error:
        if error.code == "GF_UNSUPPORTED_FILESYSTEM":
            return None
        raise


def _uuid_column(table: pa.Table, name: str) -> list[uuid.UUID]:
    column = table.column(name)
    return [uuid.UUID(bytes=column[i].as_py()) for i in range(table.num_rows)]


def check_source_artifact_lifecycle_survives_reopen() -> None:
    with tempfile.TemporaryDirectory() as root:
        if _open_admitted(root) is None:
            return

        source_uuid = uuid.uuid7()
        scan_uuid = uuid.uuid7()
        ocr_uuid = uuid.uuid7()
        preference_uuid = uuid.uuid7()

        forge = graphforge.GraphForge(root)
        forge.enable_capability(
            operation_uuid=str(uuid.UUID(int=100)),
            capability_id="provenance",
            capability_version=1,
        )
        forge.enable_capability(
            operation_uuid=str(uuid.UUID(int=101)),
            capability_id="knowledge",
            capability_version=1,
        )
        forge.register_source(
            operation_uuid=str(uuid.UUID(int=1)),
            source_uuid=str(source_uuid),
            label="Codex A",
            source_kind="manuscript",
        )
        forge.register_artifact(
            operation_uuid=str(uuid.UUID(int=2)),
            artifact_uuid=str(scan_uuid),
            source_uuid=str(source_uuid),
            artifact_kind="raw_scan",
            media_type="image/tiff",
            payload={"kind": "local_bytes", "bytes": b"scan bytes"},
        )
        forge.register_artifact(
            operation_uuid=str(uuid.UUID(int=3)),
            artifact_uuid=str(ocr_uuid),
            source_uuid=str(source_uuid),
            artifact_kind="ocr_text",
            media_type="text/plain",
            payload={"kind": "local_bytes", "bytes": b"ocr text"},
            derivation_inputs=[
                {"input_uuid": str(scan_uuid), "input_kind": "artifact"},
            ],
        )
        forge.set_preferred_artifact(
            operation_uuid=str(uuid.UUID(int=4)),
            preference_event_uuid=str(preference_uuid),
            source_uuid=str(source_uuid),
            artifact_uuid=str(scan_uuid),
            reason="initial preferred scan",
        )
        impact = forge.replacement_impact(
            source_uuid=str(source_uuid),
            artifact_uuid=str(ocr_uuid),
        )
        assert impact.num_rows == 2
        impacted = _uuid_column(impact, "artifact_uuid")
        assert scan_uuid in impacted
        assert ocr_uuid in impacted
        forge.set_preferred_artifact(
            operation_uuid=str(uuid.UUID(int=5)),
            preference_event_uuid=str(uuid.uuid7()),
            source_uuid=str(source_uuid),
            artifact_uuid=str(ocr_uuid),
            reason="better OCR available",
        )

        reopened = graphforge.GraphForge(root)
        backward = reopened.research_lineage(
            subject_uuid=str(ocr_uuid),
            subject_kind="artifact",
            direction="backward",
            max_depth=4,
        )
        assert backward.num_rows == 1
        assert _uuid_column(backward, "input_uuid") == [scan_uuid]

        closure = reopened.retention_dependency_closure(scope_uuid=str(source_uuid))
        assert closure.num_rows == 0

        post_reopen_impact = reopened.replacement_impact(
            source_uuid=str(source_uuid),
            artifact_uuid=str(scan_uuid),
        )
        assert post_reopen_impact.num_rows == 1
        assert _uuid_column(post_reopen_impact, "artifact_uuid") == [ocr_uuid]


def main() -> None:
    check_source_artifact_lifecycle_survives_reopen()


if __name__ == "__main__":
    main()
