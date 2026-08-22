"""Real retained-data #843 certification through the thin Python binding."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import tempfile
import uuid

import graphforge as g

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/multi-ontology-v1/certification-v1"


def authority(forge: g.GraphForge, seed: int) -> dict[str, object]:
    state = forge.ontology_authority_state()
    return {
        "expected_project_generation_uuid": state["project_generation_uuid"],
        "expected_composition_fingerprint": state["composition_fingerprint"],
        "operation_uuid": str(uuid.UUID(int=seed)),
    }


def substitute(value: object, identities: dict[str, dict[str, str]]) -> object:
    if isinstance(value, str) and value in identities:
        return copy.deepcopy(identities[value])
    if isinstance(value, list):
        return [substitute(item, identities) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, identities) for key, item in value.items()}
    return value


def run_certification() -> dict[str, object]:
    manifest = json.loads((FIXTURE / "certification.json").read_text())
    with tempfile.TemporaryDirectory(prefix="graphforge-certification-python-") as directory:
        project = Path(directory) / "project"
        project.mkdir()
        forge = g.GraphForge(str(project))
        identities: dict[str, dict[str, str]] = {}
        for index, filename in enumerate(manifest["modules"]):
            document = json.loads((FIXTURE / filename).read_text())
            candidate = forge.create_ontology_module(document, [])
            forge.adopt_ontology_module(candidate, **authority(forge, 843_400 + index))
            identities[f"${document['ontology_id'].rsplit('/', 1)[-1]}"] = candidate["id"]
            if filename == "genealogy-v1.json":
                forge.execute("CREATE (:Person {full_name: 'Ada Lovelace', birth_year: 1815})")

        for index, filename in enumerate(manifest["bridges"]):
            document = substitute(json.loads((FIXTURE / filename).read_text()), identities)
            candidate = forge.create_ontology_bridge(document)
            forge.adopt_ontology_bridge(candidate, **authority(forge, 843_500 + index))

        before = forge.ontology_authority_state()["composition_fingerprint"]
        genealogy = identities["$genealogy"]
        target = json.loads((FIXTURE / manifest["migration_target"]).read_text())
        operation = authority(forge, 843_600)
        preview = forge.preview_migrate_ontology_module(
            **genealogy,
            document=target,
            dependencies=[],
            enforcement=None,
            **operation,
        )
        assert preview["plan"]["retained_rows_scanned"] > 0
        receipt = forge.migrate_ontology_module(
            **genealogy,
            document=target,
            dependencies=[],
            preview=preview,
            enforcement=None,
            **operation,
        )
        forge.close()

        reopened = g.GraphForge(str(project))
        report = reopened.multi_ontology_certification_report(
            before, receipt["plan_digest"], receipt["retained_rows_scanned"]
        )
        reopened.close()
        assert report["surface"] == "python"
        assert report["retained_data"] == {
            "rows_scanned": receipt["retained_rows_scanned"],
            "name": "Ada Lovelace",
            "birth_year": 1815,
        }
        output = os.environ.get("GRAPHFORGE_MULTI_ONTOLOGY_CERTIFICATION_REPORT")
        if output:
            Path(output).write_text(
                json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n"
            )
        return report


def test_retained_data_certification_uses_rust_authority() -> None:
    report = run_certification()
    assert report["composition_before"] != report["composition_after"]
    assert len(report["module_ids"]) == 6
    assert len(report["bridge_ids"]) == 3


if __name__ == "__main__":
    test_retained_data_certification_uses_rust_authority()
