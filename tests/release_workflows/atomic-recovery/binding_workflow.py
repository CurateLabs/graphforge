"""Representative native-Python composite publish and reopen replay."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path

import graphforge
import graphforge._graphforge_rs as native

OPERATION = "018f0f4e-7b8c-7000-8000-000000050200"
NODE = "018f0f4e-7b8c-7000-8000-000000050201"
EDGE = "018f0f4e-7b8c-7000-8000-000000050202"
ASSERTION = "018f0f4e-7b8c-7000-8000-000000050211"
STATUS_EVENT = "018f0f4e-7b8c-7000-8000-000000050212"
EVIDENCE = "018f0f4e-7b8c-7000-8000-000000050213"
SOURCE = "018f0f4e-7b8c-7000-8000-000000050214"
RECORDED_AT = 1_700_000_000_000


def finding_rows(forge: graphforge.GraphForge) -> list[tuple[str, str, int]]:
    table = forge.execute(
        "MATCH (f:Finding) RETURN f.name AS name, f.claim_key AS claim_key, "
        "f.severity AS severity ORDER BY f.claim_key"
    )
    return list(
        zip(
            table.column("name").to_pylist(),
            table.column("claim_key").to_pylist(),
            table.column("severity").to_pylist(),
            strict=True,
        )
    )


def seed_project(forge: graphforge.GraphForge, ontology: Path) -> str:
    forge.load_ontology(str(ontology))
    for index, capability in enumerate(("provenance", "knowledge", "epistemic"), start=1):
        forge.enable_capability(
            operation_uuid=f"018f0f4e-7b8c-7000-8000-0000000501{index:02x}",
            capability_id=capability,
            capability_version=1,
        )
    person = forge.add_node("Person", name="Analyst Ada", role="lead", risk_score=0.2)
    forge.add_node("Organization", name="Northwind", sector="energy")
    forge.add_node("Location", name="Depot", region="NW")
    forge.add_node("Case", name="Recovery Case", case_id="CASE-2473")
    document = forge.add_node("Document", name="Memo", body="baseline note", classified=False)
    observation = forge.add_node(
        "Observation",
        name="Signal-1",
        summary="observed transfer",
        confidence=0.7,
    )
    forge.add_edge(person, "AUTHORED", document)
    forge.add_edge(document, "REFERENCES", observation)
    return str(observation.uuid)


def composite_payload(observation: str) -> tuple[list[dict], dict]:
    provenance_uuid = graphforge.composite_provenance_uuid(
        OPERATION,
        "create_assertion",
        RECORDED_AT,
    )
    mutations = [
        {
            "kind": "create_node",
            "node_uuid": NODE,
            "label": "Finding",
            "properties": {
                "name": "Binding finding",
                "claim_key": "binding-claim-1",
                "severity": 3,
            },
        },
        {
            "kind": "create_edge",
            "edge_uuid": EDGE,
            "rel_type": "SUPPORTS",
            "source_uuid": NODE,
            "target_uuid": observation,
            "properties": {"weight": 0.5},
        },
        {
            "kind": "set_edge_property",
            "edge_uuid": EDGE,
            "property": "weight",
            "value": 0.75,
        },
    ]
    knowledge = {
        "provenance_events": [
            {
                "operation_uuid": OPERATION,
                "event_kind": "create_assertion",
                "recorded_at_micros": RECORDED_AT,
                "provenance_uuid": provenance_uuid,
            }
        ],
        "lineage": [
            {
                "provenance_uuid": provenance_uuid,
                "subject_uuid": NODE,
                "subject_kind": "node",
                "role": "output",
                "ordinal": 0,
            },
            {
                "provenance_uuid": provenance_uuid,
                "subject_uuid": EDGE,
                "subject_kind": "edge",
                "role": "output",
                "ordinal": 1,
            },
            {
                "provenance_uuid": provenance_uuid,
                "subject_uuid": ASSERTION,
                "subject_kind": "assertion",
                "role": "output",
                "ordinal": 2,
            },
        ],
        "assertions": [
            {
                "assertion_uuid": ASSERTION,
                "claim": "binding composite claim",
                "provenance_uuid": provenance_uuid,
                "recorded_at_micros": RECORDED_AT,
            }
        ],
        "assertion_graph_refs": [
            {
                "assertion_uuid": ASSERTION,
                "graph_uuid": NODE,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
        "evidence": [
            {
                "evidence_uuid": EVIDENCE,
                "assertion_uuid": ASSERTION,
                "source_uuid": SOURCE,
                "source_kind": "document",
                "role": "supports",
                "weight": 0.9,
                "provenance_uuid": provenance_uuid,
                "recorded_at_micros": RECORDED_AT,
            }
        ],
        "assertion_status": [
            {
                "status_event_uuid": STATUS_EVENT,
                "assertion_uuid": ASSERTION,
                "status": "supported",
                "provenance_uuid": provenance_uuid,
                "recorded_at_micros": RECORDED_AT,
            }
        ],
    }
    return mutations, knowledge


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    args = parser.parse_args()

    if not hasattr(graphforge.GraphForge, "publish_composite_transaction"):
        raise SystemExit(
            "publish_composite_transaction is required (#2581); Python binding is unavailable"
        )
    if not hasattr(graphforge, "composite_provenance_uuid"):
        raise SystemExit(
            "composite_provenance_uuid helper is required (#2590); Python binding is unavailable"
        )

    forge = graphforge.GraphForge(str(args.project))
    ontology = Path(__file__).resolve().parent / "ontologies" / "strict-v1.yaml"
    observation = seed_project(forge, ontology)
    mutations, knowledge = composite_payload(observation)
    receipt = forge.publish_composite_transaction(
        operation_uuid=OPERATION,
        graph_mutations=mutations,
        knowledge=knowledge,
    )
    if receipt is None or receipt.num_rows != 1:
        raise AssertionError("composite publication returned no receipt")
    before = finding_rows(forge)
    if not before:
        raise AssertionError("composite publication left no Finding rows")
    forge.close()
    del forge

    # Session-scoped load_ontology is not restored by reopen; reload before typed queries
    # (same pattern as finance-fraud binding evidence).
    reopened = graphforge.GraphForge(str(args.project))
    reopened.load_ontology(str(ontology))
    after = finding_rows(reopened)
    if before != after:
        raise AssertionError("representative composite publication did not survive reopen")

    retry = reopened.publish_composite_transaction(
        operation_uuid=OPERATION,
        graph_mutations=mutations,
        knowledge=knowledge,
    )
    after_retry = finding_rows(reopened)
    if after_retry != after:
        raise AssertionError("exact composite replay mutated reopened state")
    if retry is None or retry.num_rows != 1:
        raise AssertionError("exact composite replay returned no receipt")

    extension = Path(native.__file__).resolve()
    evidence = {
        "schema_version": 1,
        "scenario_id": "atomic-recovery",
        "binding": "python",
        "commit_sha": args.commit_sha,
        "findings": [{"name": n, "claim_key": c, "severity": s} for n, c, s in after],
        "orphan_free": True,
        "reopen_equal": before == after,
        "exact_retry_identical": after_retry == after,
        "package_version": importlib.metadata.version("graphforge"),
        "native_version": graphforge.version(),
        "native_module_path": str(extension),
        "native_module_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
    }
    args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
