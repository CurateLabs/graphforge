"""Native-wheel acceptance for Python composite transactions (#2590)."""

from __future__ import annotations

import hashlib
from pathlib import Path
import subprocess
import tempfile
from uuid import UUID

import graphforge as g

OPERATION = "018f0f4e-7b8c-7000-8000-00000000d001"
NODE = "018f0f4e-7b8c-7000-8000-00000000d002"
ASSERTION = "018f0f4e-7b8c-7000-8000-00000000d003"
STATUS_EVENT = "018f0f4e-7b8c-7000-8000-00000000d004"
CONFLICT_OPERATION = "018f0f4e-7b8c-7000-8000-00000000d010"
EDGE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000d020"
EDGE = "018f0f4e-7b8c-7000-8000-00000000d021"
ONTOLOGY_OPERATION = "018f0f4e-7b8c-7000-8000-00000000d030"
ONTOLOGY_NODE = "018f0f4e-7b8c-7000-8000-00000000d031"
CAP_OPERATION = "018f0f4e-7b8c-7000-8000-00000000d040"
RECORDED_AT = 10
# Frozen aggregate entry cap from graphforge_api::MAX_COMPOSITE_TRANSACTION_ENTRIES.
MAX_COMPOSITE_TRANSACTION_ENTRIES = 100_000

RECEIPT_COLUMNS = [
    "request_identity",
    "transaction_uuid",
    "generation_uuid",
    "content_fingerprint",
    "contract_version",
    "graph_mutation_count",
    "provenance_events_count",
    "lineage_count",
    "assertions_count",
    "assertion_graph_refs_count",
    "confidence_assessments_count",
    "confidence_inputs_count",
    "evidence_count",
    "reasoning_count",
    "assertion_status_count",
    "assertion_supersessions_count",
    "hypothesis_groups_count",
    "hypothesis_membership_count",
    "hypothesis_selection_count",
    "assertion_validity_count",
]


def project_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def enable_capabilities(forge: g.GraphForge) -> None:
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-00000000d101",
        capability_id="provenance",
        capability_version=1,
    )
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-00000000d102",
        capability_id="knowledge",
        capability_version=1,
    )
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-00000000d103",
        capability_id="epistemic",
        capability_version=1,
    )


def expect_code(code: str, call) -> None:
    try:
        call()
    except g.GraphForgeError as exc:
        assert exc.code == code, exc.code
    else:
        raise SystemExit(f"expected {code}")


def composite_request(
    *,
    operation_uuid: str = OPERATION,
    node_uuid: str = NODE,
    assertion_uuid: str = ASSERTION,
    status_event_uuid: str = STATUS_EVENT,
    claim: str = "composite publishes atomically",
    graph_kind: str = "node",
) -> tuple[list[dict], dict]:
    provenance_uuid = g.composite_provenance_uuid(
        operation_uuid,
        "create_node",
        RECORDED_AT,
    )
    mutations = [
        {
            "kind": "create_node",
            "node_uuid": node_uuid,
            "label": "Person",
            "properties": {"name": "Ada"},
        }
    ]
    knowledge = {
        "provenance_events": [
            {
                "operation_uuid": operation_uuid,
                "event_kind": "create_node",
                "recorded_at_micros": RECORDED_AT,
                "provenance_uuid": provenance_uuid,
            }
        ],
        "lineage": [
            {
                "provenance_uuid": provenance_uuid,
                "subject_uuid": node_uuid,
                "subject_kind": "node",
                "role": "output",
                "ordinal": 0,
            }
        ],
        "assertions": [
            {
                "assertion_uuid": assertion_uuid,
                "claim": claim,
                "provenance_uuid": provenance_uuid,
                "recorded_at_micros": RECORDED_AT,
            }
        ],
        "assertion_graph_refs": [
            {
                "assertion_uuid": assertion_uuid,
                "graph_uuid": node_uuid,
                "graph_kind": graph_kind,
                "role": "subject",
                "ordinal": 0,
            }
        ],
        "assertion_status": [
            {
                "status_event_uuid": status_event_uuid,
                "assertion_uuid": assertion_uuid,
                "status": "supported",
                "provenance_uuid": provenance_uuid,
                "recorded_at_micros": RECORDED_AT,
            }
        ],
    }
    return mutations, knowledge


def check_no_inference_helpers() -> None:
    """Python exposes one composite publish entrypoint with no inference helpers."""
    assert hasattr(g.GraphForge, "publish_composite_transaction")
    assert not hasattr(g.GraphForge, "publish_composite_graph_transaction")
    assert not hasattr(g.GraphForge, "publish_composite_knowledge_transaction")
    assert not hasattr(g, "infer_composite_participants")
    assert not hasattr(g.GraphForge, "infer_composite_participants")


def check_composite_transaction(project: Path) -> None:
    forge = g.GraphForge(str(project))
    enable_capabilities(forge)

    mutations, knowledge = composite_request()
    receipt = forge.publish_composite_transaction(
        operation_uuid=OPERATION,
        graph_mutations=mutations,
        knowledge=knowledge,
    )
    assert receipt.num_rows == 1, receipt
    assert receipt.column_names == RECEIPT_COLUMNS, receipt.column_names
    metadata = receipt.schema.metadata or {}
    assert metadata.get(b"graphforge.composite_kind") == b"receipt"
    assert metadata.get(b"graphforge.row_order") == b"singleton"
    assert receipt.column("graph_mutation_count").to_pylist() == [1]
    assert receipt.column("provenance_events_count").to_pylist() == [1]
    assert receipt.column("lineage_count").to_pylist() == [1]
    assert receipt.column("assertions_count").to_pylist() == [1]
    assert receipt.column("assertion_graph_refs_count").to_pylist() == [1]
    assert receipt.column("assertion_status_count").to_pylist() == [1]
    assert receipt.column("evidence_count").to_pylist() == [0]
    assert receipt.column("contract_version").to_pylist() == [1]
    assert receipt.column("request_identity").to_pylist() == [UUID(OPERATION).bytes]

    names = forge.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
    assert names.column("name").to_pylist() == ["Ada"]
    assertions = forge.list_assertions()
    assert assertions.num_rows == 1
    assert assertions.column("assertion_uuid").to_pylist() == [UUID(ASSERTION).bytes]
    status = forge.list_assertion_status()
    assert status.num_rows == 1
    assert status.column("status_event_uuid").to_pylist() == [UUID(STATUS_EVENT).bytes]

    before = project_digest(project)
    retry = forge.publish_composite_transaction(
        operation_uuid=OPERATION,
        graph_mutations=mutations,
        knowledge=knowledge,
    )
    assert (
        retry.column("generation_uuid").to_pylist() == receipt.column("generation_uuid").to_pylist()
    )
    assert (
        retry.column("content_fingerprint").to_pylist()
        == receipt.column("content_fingerprint").to_pylist()
    )
    assert project_digest(project) == before

    conflict_mutations, conflict_knowledge = composite_request(claim="different claim")
    expect_code(
        "GF_IDEMPOTENCY_CONFLICT",
        lambda: forge.publish_composite_transaction(
            operation_uuid=OPERATION,
            graph_mutations=conflict_mutations,
            knowledge=conflict_knowledge,
        ),
    )
    assert project_digest(project) == before

    forge = g.GraphForge(str(project))
    reopened = forge.execute("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
    assert reopened.column("name").to_pylist() == ["Ada"]
    assert forge.list_assertions().num_rows == 1
    assert forge.list_assertion_status().num_rows == 1
    assert forge.list_assertions().column("assertion_uuid").to_pylist() == [UUID(ASSERTION).bytes]

    before = project_digest(project)
    bad_mutations, bad_knowledge = composite_request(
        operation_uuid=CONFLICT_OPERATION,
        node_uuid="018f0f4e-7b8c-7000-8000-00000000d012",
        assertion_uuid="018f0f4e-7b8c-7000-8000-00000000d013",
        status_event_uuid="018f0f4e-7b8c-7000-8000-00000000d014",
        graph_kind="edge",
    )
    expect_code(
        "GF_NOT_FOUND",
        lambda: forge.publish_composite_transaction(
            operation_uuid=CONFLICT_OPERATION,
            graph_mutations=bad_mutations,
            knowledge=bad_knowledge,
        ),
    )
    assert project_digest(project) == before

    try:
        forge.publish_composite_transaction(
            operation_uuid="018f0f4e-7b8c-7000-8000-00000000e020",
            graph_mutations=[{"kind": "unknown_mutation"}],
        )
    except TypeError as exc:
        assert "unknown composite graph mutation kind" in str(exc)
    else:
        raise SystemExit("expected an unknown composite mutation TypeError")
    assert project_digest(project) == before

    # Invalid identity (nil operation UUID) stays in Rust validation.
    expect_code(
        "GF_VALIDATION",
        lambda: forge.publish_composite_transaction(
            operation_uuid="00000000-0000-0000-0000-000000000000",
            graph_mutations=[
                {
                    "kind": "create_node",
                    "node_uuid": "018f0f4e-7b8c-7000-8000-00000000d015",
                    "label": "Person",
                    "properties": {"name": "Nil"},
                }
            ],
        ),
    )
    assert project_digest(project) == before

    # Missing edge endpoints produce the same stable Rust not-found error.
    expect_code(
        "GF_NOT_FOUND",
        lambda: forge.publish_composite_transaction(
            operation_uuid=EDGE_OPERATION,
            graph_mutations=[
                {
                    "kind": "create_edge",
                    "edge_uuid": EDGE,
                    "rel_type": "KNOWS",
                    "source_uuid": "018f0f4e-7b8c-7000-8000-00000000d0aa",
                    "target_uuid": "018f0f4e-7b8c-7000-8000-00000000d0ab",
                }
            ],
        ),
    )
    assert project_digest(project) == before

    # Aggregate-cap overflow is rejected with zero mutation.
    overflow = [
        {
            "kind": "delete_node",
            "node_uuid": "018f0f4e-7b8c-7000-8000-00000000d0ff",
        }
        for _ in range(MAX_COMPOSITE_TRANSACTION_ENTRIES + 1)
    ]
    expect_code(
        "GF_VALIDATION",
        lambda: forge.publish_composite_transaction(
            operation_uuid=CAP_OPERATION,
            graph_mutations=overflow,
        ),
    )
    assert project_digest(project) == before
    assert forge.execute("MATCH (n:Person) RETURN count(n) AS c").column("c").to_pylist() == [1]

    # Domain content limit: oversized reasoning is rejected by Rust constructors.
    oversized = "x" * 65_537
    expect_code(
        "GF_VALIDATION",
        lambda: forge.publish_composite_transaction(
            operation_uuid="018f0f4e-7b8c-7000-8000-00000000d050",
            graph_mutations=[
                {
                    "kind": "create_node",
                    "node_uuid": "018f0f4e-7b8c-7000-8000-00000000d051",
                    "label": "Person",
                    "properties": {"name": "Limit"},
                }
            ],
            knowledge={
                "reasoning": [
                    {
                        "reasoning_uuid": "018f0f4e-7b8c-7000-8000-00000000d052",
                        "assertion_uuid": ASSERTION,
                        "kind": "methodological_note",
                        "content_format": "text/plain",
                        "content": oversized,
                        "provenance_uuid": g.composite_provenance_uuid(
                            "018f0f4e-7b8c-7000-8000-00000000d050",
                            "create_assertion",
                            RECORDED_AT,
                        ),
                        "recorded_at_micros": RECORDED_AT,
                    }
                ]
            },
        ),
    )
    assert project_digest(project) == before


def check_strict_ontology_composite(project: Path) -> None:
    forge = g.GraphForge(str(project))
    assert forge.ontology_mode == "strict", forge.ontology_mode
    enable_capabilities(forge)
    before = project_digest(project)
    mutations, knowledge = composite_request(
        operation_uuid=ONTOLOGY_OPERATION,
        node_uuid=ONTOLOGY_NODE,
        assertion_uuid="018f0f4e-7b8c-7000-8000-00000000d032",
        status_event_uuid="018f0f4e-7b8c-7000-8000-00000000d033",
    )
    expect_code(
        "GF_ONTOLOGY",
        lambda: forge.publish_composite_transaction(
            operation_uuid=ONTOLOGY_OPERATION,
            graph_mutations=mutations,
            knowledge=knowledge,
        ),
    )
    assert project_digest(project) == before


def check_all_explicit_participant_conversions(project: Path) -> None:
    """Every documented participant family crosses the thin adapter."""
    project.mkdir()
    forge = g.GraphForge(str(project))
    enable_capabilities(forge)
    operation = "018f0f4e-7b8c-7000-8000-00000000e001"
    provenance = "018f0f4e-7b8c-7000-8000-00000000e002"
    assertion = "018f0f4e-7b8c-7000-8000-00000000e003"
    confidence = "018f0f4e-7b8c-7000-8000-00000000e004"
    reasoning = "018f0f4e-7b8c-7000-8000-00000000e005"
    group = "018f0f4e-7b8c-7000-8000-00000000e006"
    before = project_digest(project)

    # These rows are structurally valid at the language boundary but deliberately
    # reference an absent provenance event. Rust must reject the entire request
    # without publishing the graph mutation.
    expect_code(
        "GF_NOT_FOUND",
        lambda: forge.publish_composite_transaction(
            operation_uuid=operation,
            graph_mutations=[
                {
                    "kind": "create_node",
                    "node_uuid": "018f0f4e-7b8c-7000-8000-00000000e010",
                    "label": "Person",
                    "properties": {"name": "Grace"},
                }
            ],
            knowledge={
                "confidence_assessments": [
                    {
                        "confidence_uuid": confidence,
                        "assertion_uuid": assertion,
                        "policy": "conservative_min",
                        "value": 0.5,
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
                "confidence_inputs": [
                    {
                        "confidence_uuid": confidence,
                        "input_confidence_uuid": "018f0f4e-7b8c-7000-8000-00000000e007",
                        "input_value": 0.5,
                        "ordinal": 0,
                    }
                ],
                "evidence": [
                    {
                        "evidence_uuid": "018f0f4e-7b8c-7000-8000-00000000e008",
                        "assertion_uuid": assertion,
                        "source_uuid": "018f0f4e-7b8c-7000-8000-00000000e009",
                        "source_kind": "document",
                        "role": "supports",
                        "weight": 0.8,
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
                "reasoning": [
                    {
                        "reasoning_uuid": reasoning,
                        "assertion_uuid": assertion,
                        "kind": "methodological_note",
                        "content_format": "text/plain",
                        "content": b"explicit participant conversion",
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
                "assertion_supersessions": [
                    {
                        "supersession_uuid": "018f0f4e-7b8c-7000-8000-00000000e00a",
                        "prior_assertion_uuid": assertion,
                        "replacement_assertion_uuid": "018f0f4e-7b8c-7000-8000-00000000e00b",
                        "status_event_uuid": "018f0f4e-7b8c-7000-8000-00000000e00c",
                        "reasoning_uuid": reasoning,
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
                "hypothesis_groups": [
                    {
                        "group_uuid": group,
                        "question_key": "which hypothesis",
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
                "hypothesis_membership": [
                    {
                        "membership_event_uuid": "018f0f4e-7b8c-7000-8000-00000000e00d",
                        "operation_uuid": operation,
                        "group_uuid": group,
                        "assertion_uuid": assertion,
                        "action": "added",
                        "reasoning_uuid": reasoning,
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
                "hypothesis_selection": [
                    {
                        "selection_event_uuid": "018f0f4e-7b8c-7000-8000-00000000e00e",
                        "operation_uuid": operation,
                        "group_uuid": group,
                        "selected_assertion_uuid": assertion,
                        "reasoning_uuid": reasoning,
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
                "assertion_validity": [
                    {
                        "validity_event_uuid": "018f0f4e-7b8c-7000-8000-00000000e00f",
                        "assertion_uuid": assertion,
                        "valid_from": 1,
                        "valid_to": 2,
                        "reasoning_uuid": reasoning,
                        "provenance_uuid": provenance,
                        "recorded_at_micros": RECORDED_AT,
                    }
                ],
            },
        ),
    )
    assert project_digest(project) == before


if __name__ == "__main__":
    check_no_inference_helpers()
    with tempfile.TemporaryDirectory(prefix="gf-composite-py-") as directory:
        check_composite_transaction(Path(directory))
        check_all_explicit_participant_conversions(Path(directory) / "participants")
        onto_root = Path(directory) / "strict"
        onto_root.mkdir()
        project = onto_root / "project"
        ontology = onto_root / "ontology.yaml"
        ontology.write_text(
            'ontology_id: composite\nversion: "1"\nentity_types:\n  - name: Organization\n',
            encoding="utf-8",
        )
        subprocess.run(
            [
                "cargo",
                "run",
                "--quiet",
                "-p",
                "graphforge-api",
                "--example",
                "strict_add_node_fixture",
                "--",
                str(project),
                str(ontology),
            ],
            cwd=Path(__file__).resolve().parents[3],
            check=True,
        )
        check_strict_ontology_composite(project)
        print("composite_transaction: ok")
