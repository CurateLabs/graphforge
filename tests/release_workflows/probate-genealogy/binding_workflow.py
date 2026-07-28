"""Python-binding repetition of the #2466 logical workflow."""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
from pathlib import Path
import uuid

import graphforge
import graphforge._graphforge_rs as native


def uid(suffix: int) -> str:
    return f"018f0f4e-7b8c-7000-8000-00000002{suffix:04x}"


def provenance(table: object) -> str:
    value = table.column("provenance_uuid").to_pylist()[0]
    return str(uuid.UUID(bytes=value))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--ontology", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()

    args.project.mkdir(parents=True)
    forge = graphforge.GraphForge(str(args.project))
    forge.load_ontology(str(args.ontology))
    for suffix, capability in [
        (0x0100, "provenance"),
        (0x0101, "knowledge"),
        (0x0102, "epistemic"),
    ]:
        forge.enable_capability(
            operation_uuid=uid(suffix),
            capability_id=capability,
            capability_version=1,
        )

    person = forge.add_node("Person", name="Ada North", birth_year=1912)
    assertions: list[tuple[str, str, str]] = []
    for offset, claim in [
        (0x0200, "Bea North is Ada North's recorded parent"),
        (0x0210, "Cora Vale is Ada North's recorded parent"),
    ]:
        assertion_uuid = uid(offset)
        row = forge.create_assertion(
            operation_uuid=uid(offset + 1),
            assertion_uuid=assertion_uuid,
            claim=claim,
            graph_refs=[
                {
                    "graph_uuid": person.uuid,
                    "graph_kind": "node",
                    "role": "subject",
                    "ordinal": 0,
                }
            ],
        )
        provenance_uuid = provenance(row)
        reasoning_uuid = uid(offset + 2)
        forge.record_reasoning(
            operation_uuid=uid(offset + 3),
            reasoning_uuid=reasoning_uuid,
            assertion_uuid=assertion_uuid,
            kind="evidence_interpretation",
            content_format="text/plain",
            content=claim.encode(),
            provenance_uuid=provenance_uuid,
        )
        forge.record_assertion_status(
            operation_uuid=uid(offset + 4),
            status_event_uuid=uid(offset + 5),
            assertion_uuid=assertion_uuid,
            status="hypothesis",
            provenance_uuid=provenance_uuid,
            reasoning_uuid=reasoning_uuid,
        )
        assertions.append((assertion_uuid, reasoning_uuid, provenance_uuid))

    group = uid(0x0300)
    forge.create_hypothesis_group(
        operation_uuid=uid(0x0301),
        group_uuid=group,
        question_key="probate.ada-parentage.v1",
        provenance_uuid=assertions[0][2],
    )
    for index, (assertion_uuid, reasoning_uuid, provenance_uuid) in enumerate(assertions):
        forge.record_hypothesis_membership(
            operation_uuid=uid(0x0310 + index * 2),
            membership_event_uuid=uid(0x0311 + index * 2),
            group_uuid=group,
            assertion_uuid=assertion_uuid,
            action="added",
            reasoning_uuid=reasoning_uuid,
            provenance_uuid=provenance_uuid,
        )
    if forge.hypothesis_selection(group).num_rows != 0:
        raise AssertionError("expected no hypothesis selection before selection events")
    for index, selected in enumerate([assertions[0], assertions[1]]):
        forge.record_hypothesis_selection(
            operation_uuid=uid(0x0320 + index * 2),
            selection_event_uuid=uid(0x0321 + index * 2),
            group_uuid=group,
            selected_assertion_uuid=selected[0],
            reasoning_uuid=selected[1],
            provenance_uuid=selected[2],
        )
    forge.record_hypothesis_selection(
        operation_uuid=uid(0x0330),
        selection_event_uuid=uid(0x0331),
        group_uuid=group,
        selected_assertion_uuid=None,
        reasoning_uuid=assertions[1][1],
        provenance_uuid=assertions[1][2],
    )
    members_count = forge.hypothesis_members(group).num_rows
    selection_events_count = forge.list_hypothesis_selection(group_uuid=group).num_rows
    selected = forge.hypothesis_selection(group)
    current_selection = selected.column("selected_assertion_uuid").to_pylist()
    unselected_statuses = [
        forge.assertion_status(assertion_uuid).column("status").to_pylist()[0]
        for assertion_uuid, _, _ in assertions
    ]
    if members_count != 2:
        raise AssertionError(f"expected 2 hypothesis members, got {members_count}")
    if selection_events_count != 3:
        raise AssertionError(f"expected 3 selection events, got {selection_events_count}")
    if current_selection != [None]:
        raise AssertionError(f"expected cleared selection, got {current_selection}")
    if unselected_statuses != ["hypothesis", "hypothesis"]:
        raise AssertionError(f"unexpected hypothesis statuses: {unselected_statuses}")

    del forge
    gc.collect()
    reopened = graphforge.GraphForge(str(args.project))
    reopened.load_ontology(str(args.ontology))
    reopened_members_count = reopened.hypothesis_members(group).num_rows
    reopened_selection_events_count = reopened.list_hypothesis_selection(group_uuid=group).num_rows
    reopened_selection = (
        reopened.hypothesis_selection(group).column("selected_assertion_uuid").to_pylist()
    )
    reopened_names = (
        reopened.execute("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .column("name")
        .to_pylist()
    )
    reopen_identical = (
        reopened_members_count == members_count
        and reopened_selection_events_count == selection_events_count
        and reopened_selection == current_selection
        and reopened_names == ["Ada North"]
    )
    if not reopen_identical:
        raise AssertionError("reopened project does not match the pre-close workflow state")

    evidence = {
        "schema_version": 1,
        "scenario_id": "probate-genealogy",
        "binding": "python",
        "logical_repeat": [
            "create competing assertions",
            "select first",
            "change to second",
            "clear selection",
            "close and reopen",
        ],
        "assertion_uuids": [item[0] for item in assertions],
        "hypothesis_group_uuid": group,
        "members": members_count,
        "selection_events": selection_events_count,
        "current_selection": current_selection[0],
        "unselected_statuses": unselected_statuses,
        "reopen_identical": reopen_identical,
        "package_version": importlib.metadata.version("graphforge"),
        "native_version": graphforge.version(),
        "native_module_path": str(Path(native.__file__).resolve()),
        "native_module_sha256": hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
    }
    args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
