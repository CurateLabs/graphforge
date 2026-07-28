"""Representative native-Python knowledge selection and reopen replay."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import uuid

import graphforge
import graphforge._graphforge_rs as native


def uid(suffix: int) -> str:
    return f"018f0f4e-7b8c-7000-8000-00000005{suffix:04x}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    args = parser.parse_args()
    args.project.mkdir(parents=True)
    forge = graphforge.GraphForge(str(args.project))
    for suffix, capability in [(1, "provenance"), (2, "knowledge"), (3, "epistemic")]:
        forge.enable_capability(
            operation_uuid=uid(suffix), capability_id=capability, capability_version=1
        )
    node = forge.add_node("Observation", name="Observation Alpha", summary="Stable graph")
    assertions: list[tuple[str, str, str]] = []
    for offset, claim in [(0x100, "Explanation Alpha"), (0x110, "Explanation Beta")]:
        assertion = uid(offset)
        row = forge.create_assertion(
            operation_uuid=uid(offset + 1),
            assertion_uuid=assertion,
            claim=claim,
            graph_refs=[
                {"graph_uuid": node.uuid, "graph_kind": "node", "role": "subject", "ordinal": 0}
            ],
        )
        provenance = str(uuid.UUID(bytes=row.column("provenance_uuid").to_pylist()[0]))
        reasoning = uid(offset + 2)
        forge.record_reasoning(
            operation_uuid=uid(offset + 3),
            reasoning_uuid=reasoning,
            assertion_uuid=assertion,
            kind="evidence_interpretation",
            content_format="text/plain",
            content=claim.encode(),
            provenance_uuid=provenance,
        )
        assertions.append((assertion, reasoning, provenance))
    group = uid(0x200)
    forge.create_hypothesis_group(
        operation_uuid=uid(0x201),
        group_uuid=group,
        question_key="knowledge-evolution.explanation.v1",
        provenance_uuid=assertions[0][2],
    )
    for index, (assertion, reasoning, provenance) in enumerate(assertions):
        forge.record_hypothesis_membership(
            operation_uuid=uid(0x204 + index * 2),
            membership_event_uuid=uid(0x205 + index * 2),
            group_uuid=group,
            assertion_uuid=assertion,
            action="added",
            reasoning_uuid=reasoning,
            provenance_uuid=provenance,
        )
    # Representative selection/change/clear. Rust owns complete evidence semantics.
    for index, selected in enumerate([assertions[0][0], assertions[1][0], None]):
        supporting = assertions[min(index, 1)]
        forge.record_hypothesis_selection(
            operation_uuid=uid(0x210 + index * 2),
            selection_event_uuid=uid(0x211 + index * 2),
            group_uuid=group,
            selected_assertion_uuid=selected,
            reasoning_uuid=supporting[1],
            provenance_uuid=supporting[2],
        )
    events = forge.list_hypothesis_selection(group_uuid=group).num_rows
    current = forge.hypothesis_selection(group).column("selected_assertion_uuid").to_pylist()
    del forge
    reopened = graphforge.GraphForge(str(args.project))
    reopen_current = (
        reopened.hypothesis_selection(group).column("selected_assertion_uuid").to_pylist()
    )
    extension = Path(native.__file__).resolve()
    args.evidence.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "scenario_id": "knowledge-evolution",
                "binding": "python",
                "commit_sha": args.commit_sha,
                "selection_events": events,
                "current_selection": current,
                "reopen_equal": reopen_current == current,
                "package_version": importlib.metadata.version("graphforge"),
                "native_version": graphforge.version(),
                "native_module_path": str(extension),
                "native_module_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
