"""Native Python ontology lifecycle parity and persistence acceptance (#237)."""

from collections.abc import Callable
import json
from pathlib import Path
import tempfile

import graphforge as g

ROOT = Path(__file__).parents[3]
EXPECTED = ROOT / "tests/contracts/ontology-lifecycle-draft.json"
EXPECTED_YAML = ROOT / "tests/contracts/ontology-lifecycle-draft.yaml"
ADOPT = "018f5f0d-65dd-7a88-b6ef-0123456789ab"
CLEAR = "018f5f0d-65dd-7a88-b6ef-0123456789ac"


def expect_error(
    call: Callable[[], None], expected_type: type[Exception], expected_code: str
) -> None:
    try:
        call()
    except expected_type as error:
        assert error.code == expected_code
    else:
        raise AssertionError(f"expected {expected_type.__name__}")


def main() -> None:
    forge = g.GraphForge()
    forge.execute("CREATE (:Person {name: 'Alice'})")
    catalog = forge.inspect_runtime_catalog()
    assert catalog["contract_version"] == 1
    assert [(entry["kind"], entry["name"]) for entry in catalog["entries"]] == [
        ("entity_type", "Person"),
    ]

    suggestion = forge.suggest_ontology("binding-parity", "1.0.0")
    expected = json.loads(EXPECTED.read_text())
    assert suggestion["draft"] is True
    assert suggestion["document"] == expected
    assert suggestion["omitted_relation_types"] == []
    assert forge.validate_ontology(expected) == {"valid": True, "diagnostics": []}

    with tempfile.TemporaryDirectory(prefix="graphforge-ontology-py-") as directory:
        root = Path(directory)
        exported = root / "suggested.json"
        forge.export_ontology("suggested", str(exported), "json", document=suggestion["document"])
        expected_bytes = EXPECTED.read_bytes().rstrip(b"\n")
        assert exported.read_bytes() == expected_bytes
        exported_yaml = root / "suggested.yaml"
        forge.export_ontology(
            "suggested", str(exported_yaml), "yaml", document=suggestion["document"]
        )
        assert exported_yaml.read_bytes() == EXPECTED_YAML.read_bytes()

        for error_case in [
            lambda: expect_error(
                lambda: forge.export_ontology("invalid", str(exported), "json"),
                g.ValidationError,
                "GF_VALIDATION",
            ),
            lambda: expect_error(
                lambda: forge.export_ontology("suggested", str(exported), "xml"),
                g.ValidationError,
                "GF_VALIDATION",
            ),
            lambda: expect_error(
                lambda: forge.export_ontology(
                    "suggested",
                    str(root / "missing" / "out.json"),
                    "json",
                    document=suggestion["document"],
                ),
                g.StorageError,
                "GF_IO",
            ),
        ]:
            error_case()

        project = root / "project"
        project.mkdir()
        session = g.GraphForge(str(project))
        session.load_ontology(str(EXPECTED))
        assert session.ontology_mode == "advisory"
        session.close()
        reopened = g.GraphForge(str(project))
        assert reopened.ontology_mode == "exploratory"

        reopened.adopt_ontology(str(EXPECTED), "strict", operation_uuid=ADOPT)
        reopened.adopt_ontology(str(EXPECTED), "strict", operation_uuid=ADOPT)
        assert reopened.workspace_ontology()["mode"] == "strict"
        try:
            reopened.adopt_ontology(str(EXPECTED), "advisory", operation_uuid=ADOPT)
        except g.StorageError as error:
            assert error.code == "GF_IDEMPOTENCY_CONFLICT"
        else:
            raise AssertionError("expected idempotency conflict")
        try:
            reopened.adopt_ontology(
                str(EXPECTED),
                "exploratory",
                operation_uuid="018f5f0d-65dd-7a88-b6ef-0123456789ad",
            )
        except g.ValidationError as error:
            assert error.code == "GF_VALIDATION"
        else:
            raise AssertionError("expected invalid ontology mode")
        reopened.close()

        adopted = g.GraphForge(str(project))
        assert adopted.ontology_mode == "strict"
        adopted_export = root / "adopted.json"
        adopted.export_ontology("adopted", str(adopted_export), "json")
        assert adopted_export.read_bytes() == expected_bytes
        adopted.clear_ontology(operation_uuid=CLEAR)
        adopted.clear_ontology(operation_uuid=CLEAR)
        assert adopted.workspace_ontology()["mode"] == "none"
        adopted.close()

        cleared = g.GraphForge(str(project))
        assert cleared.ontology_mode == "exploratory"
        assert cleared.workspace_ontology()["canonical_ontology"] is None

    print("python ontology lifecycle parity passed")


if __name__ == "__main__":
    main()
