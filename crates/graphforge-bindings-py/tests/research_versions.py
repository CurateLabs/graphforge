"""Real Python parity for immutable Version capture, retention and restoration."""

from pathlib import Path
import tempfile
import unittest

import graphforge


def identity(number: int) -> str:
    return f"018f0f4e-7b8c-7000-8000-{number:012d}"


def check_research_versions() -> None:
    """Freeze graph/ontology/evidence, compact, reopen and restore through Rust."""
    with tempfile.TemporaryDirectory() as directory:
        root = str(Path(directory) / "project")
        graph = graphforge.GraphForge(root)
        graph.execute("CREATE (:Person)")
        ontology = Path(directory) / "ontology.yaml"
        ontology.write_text(
            'ontology_id: historical\nversion: "1"\nentity_types:\n'
            "  - name: Person\n    abstract: false\nrelation_types: []\n"
        )
        graph.adopt_ontology(str(ontology), mode="advisory", operation_uuid=identity(1))
        for number, capability in [(2, "provenance"), (3, "knowledge")]:
            graph.enable_capability(
                operation_uuid=identity(number), capability_id=capability, capability_version=1
            )
        graph.register_source(
            operation_uuid=identity(4),
            source_uuid=identity(5),
            label="Original",
            source_kind="manuscript",
        )
        graph.register_artifact(
            operation_uuid=identity(6),
            artifact_uuid=identity(7),
            source_uuid=identity(5),
            artifact_kind="raw_scan",
            media_type="application/octet-stream",
            payload={"kind": "local_bytes", "bytes": b"frozen bytes"},
        )
        frozen_ontology = graph.workspace_ontology()
        metadata = graph.research_project_metadata()
        metadata["title"] = "Frozen"
        graph.update_research_metadata(metadata, operation_uuid=identity(8))
        prepared = graph.prepare_research_version(
            {
                "operation_uuid": identity(9),
                "version_uuid": identity(10),
                "context_uuid": identity(11),
                "label": "Citation",
                "description": None,
                "created_at": 1,
                "required_versions": [],
            }
        )
        receipt = graph.commit_research_version_operation(prepared)
        graph.checkpoint(name="Independent", idempotency_key=identity(12))
        graph.execute("CREATE (:Person)")
        graph.delete_checkpoint(name="Independent", idempotency_key=identity(13))
        metadata["title"] = "Later"
        graph.update_research_metadata(metadata, operation_uuid=identity(14))
        graph.commit_research_version_operation(
            {
                "operation_uuid": identity(15),
                "expected_generation_uuid": graph.research_project_summary()["identity"][
                    "generation_uuid"
                ],
                "mutation": {"operation": "compact", "versions": [identity(10)]},
            }
        )
        assert graph.research_version_artifact(identity(10), identity(7)).num_rows == 1
        frozen_record = graph.research_version(identity(10))
        graph.close()
        graph = graphforge.GraphForge(root)
        assert graph.research_version(identity(10)) == frozen_record
        assert (
            graph.query_research_version(identity(10), "MATCH (n) RETURN count(n)")
            .column(0)[0]
            .as_py()
            == 1
        )
        assert graph.research_version_ontology(identity(10)) == frozen_ontology
        assert graph.research_version_metadata(identity(10))["title"] == "Frozen"
        assert (
            graph.research_version_artifact_payload(identity(10), identity(7)).column(0)[0].as_py()
            == b"frozen bytes"
        )
        restore = {
            "operation_uuid": identity(16),
            "expected_generation_uuid": graph.research_project_summary()["identity"][
                "generation_uuid"
            ],
            "mutation": {
                "operation": "restore_project",
                "context_uuid": identity(11),
                "source_version": identity(10),
                "version_uuid": identity(17),
                "created_at": 2,
            },
        }
        restored = graph.commit_research_version_operation(restore)
        assert graph.execute("MATCH (n) RETURN count(n)").column(0)[0].as_py() == 1
        graph.execute("CREATE (:Person)")
        assert graph.commit_research_version_operation(restore) == restored
        assert graph.commit_research_version_operation(prepared) == receipt
        assert graph.execute("MATCH (n) RETURN count(n)").column(0)[0].as_py() == 2
        assert graph.list_research_versions().num_rows == 2
        cancellation = graphforge.CancellationToken()
        cancellation.cancel()
        try:
            graph.commit_research_version_operation(prepared, cancellation=cancellation)
        except Exception as error:
            assert error.code == "GF_CANCELLED"
        else:
            raise AssertionError("cancelled commit succeeded")
        graph.close()


def check_private_request_diagnostics() -> None:
    graph = graphforge.GraphForge()
    sentinel = "PRIVATE_SOURCE_SENTINEL"
    valid = {
        "operation_uuid": identity(101),
        "version_uuid": identity(102),
        "context_uuid": identity(103),
        "created_at": 1,
        "required_versions": [],
    }
    try:
        for request in [dict(valid, created_at=sentinel), dict(valid, **{sentinel: True})]:
            with unittest.TestCase().assertRaises(Exception) as raised:
                graph.prepare_research_version(request)
            assert sentinel not in str(raised.exception) and len(str(raised.exception)) < 256
        prepared = graph.prepare_research_version(valid)
        try:
            graph.commit_research_version_operation(
                dict(prepared, mutation={"operation": sentinel})
            )
        except Exception as error:
            assert sentinel not in str(error)
        else:
            raise AssertionError("invalid operation accepted")
    finally:
        graph.close()


def main() -> None:
    check_private_request_diagnostics()
    check_research_versions()


if __name__ == "__main__":
    main()
