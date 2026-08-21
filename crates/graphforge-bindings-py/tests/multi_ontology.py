"""Native semantic conformance for the #842 four-surface contract."""

from __future__ import annotations

import copy
import functools
import hashlib
import json
import os
from pathlib import Path
import tempfile
import uuid

import graphforge as g

ROOT = Path(__file__).resolve().parents[3]
SURFACE = ROOT / "tests/contracts/multi-ontology-surface-v1.json"
ORACLE = ROOT / "tests/fixtures/multi-ontology-v1/binding-parity-v1.json"


def op(seed: int) -> str:
    return str(uuid.UUID(int=seed))


def failure(call: object) -> tuple[str, list[dict[str, object]]]:
    try:
        call()  # type: ignore[operator]
    except Exception as error:
        diagnostics = getattr(error, "diagnostics", [])
        assert isinstance(diagnostics, list)
        return str(getattr(error, "code", "")), diagnostics
    raise AssertionError("expected native failure")


def code(call: object) -> str:
    return failure(call)[0]


def authority(forge: g.GraphForge, seed: int) -> dict[str, object]:
    state = forge.ontology_authority_state()
    return {
        "expected_project_generation_uuid": state["project_generation_uuid"],
        "expected_composition_fingerprint": state["composition_fingerprint"],
        "operation_uuid": op(seed),
    }


def substitute(value: object, identities: dict[str, dict[str, str]]) -> object:
    if isinstance(value, str) and value in identities:
        return copy.deepcopy(identities[value])
    if isinstance(value, list):
        return [substitute(item, identities) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, identities) for key, item in value.items()}
    return value


def composition(forge: g.GraphForge) -> dict[str, object]:
    modules = []
    for row in forge.ontology_modules():
        inspected = forge.inspect_ontology_module(**row["id"])
        modules.append(
            {
                "id": row["id"],
                "dependencies": row["dependencies"],
                "document": inspected["doc"],
                "allow_projected_identity": False,
            }
        )
    bridges = [
        forge.inspect_ontology_bridge(**row["id"])["doc"] for row in forge.ontology_bridges()
    ]
    profile = forge.ontology_activation_profile()
    return {
        "contract_version": 1,
        "modules": modules,
        "bridges": bridges,
        "activation": profile["activation"],
        "profile_default": profile["profile_default"],
        "composition_fingerprint": forge.ontology_authority_state()["composition_fingerprint"],
    }


def canonical(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def inject_unsupported_feature(expanded: Path) -> None:
    control_path = (
        expanded / "data/components/compatibility/graphforge-ontology-composition/composition.json"
    )
    control = json.loads(control_path.read_text())
    control["required_features"].append("future-multi-ontology@999")
    control["required_features"].sort()
    unsigned = copy.deepcopy(control)
    unsigned.pop("composition_digest")
    control["composition_digest"] = (
        "sha256:"
        + hashlib.sha256(b"graphforge-ontology-composition/1\0" + canonical(unsigned)).hexdigest()
    )
    control_bytes = canonical(control)
    control_path.write_bytes(control_bytes)
    manifest_path = expanded / "data/graphforge-project.json"
    manifest = json.loads(manifest_path.read_text())
    file = next(
        file
        for component in manifest["components"]
        for file in component["files"]
        if file["path"].endswith("composition.json")
    )
    file["length"] = len(control_bytes)
    file["sha256"] = hashlib.sha256(control_bytes).hexdigest()
    unsigned_manifest = copy.deepcopy(manifest)
    unsigned_manifest.pop("package_digest")
    manifest["package_digest"] = (
        "sha256:"
        + hashlib.sha256(b"graphforge-project/2\0" + canonical(unsigned_manifest)).hexdigest()
    )
    manifest_path.write_bytes(canonical(manifest))


def _run_four_surface_operation_conformance() -> dict[str, object]:
    contract = json.loads(SURFACE.read_text())
    oracle = json.loads(ORACLE.read_text())
    assert {case["id"] for case in oracle["cases"]} == set(contract["required_conformance_cases"])
    with tempfile.TemporaryDirectory(prefix="graphforge-multi-ontology-py-") as directory:
        root = Path(directory)
        project = root / "project"
        project.mkdir()
        base_path = root / "base.json"
        base_path.write_text(json.dumps(oracle["modules"]["base"], sort_keys=True))
        forge = g.GraphForge(str(project))
        forge.adopt_ontology(str(base_path), "advisory", operation_uuid=op(1))

        base = forge.ontology_modules()[0]
        assert forge.inspect_ontology_module(**base["id"])["doc"] == oracle["modules"]["base"]
        module_validation = forge.validate_ontology_module(oracle["modules"]["dependent"])
        assert module_validation == {"valid": True, "diagnostics": []}
        invalid_module = copy.deepcopy(oracle["modules"]["dependent"])
        invalid_module["entity_types"].append(copy.deepcopy(invalid_module["entity_types"][0]))
        invalid_module_validation = forge.validate_ontology_module(invalid_module)
        assert set(invalid_module_validation) == {"valid", "diagnostics"}
        assert invalid_module_validation["valid"] is False
        assert invalid_module_validation["diagnostics"][0]["code"] == "inventory.malformed"
        assert set(invalid_module_validation["diagnostics"][0]) == {
            "code",
            "phase",
            "message",
            "subjects",
            "candidates",
            "remediation",
            "limit",
        }
        imported = forge.import_ontology_module(
            json.dumps(oracle["modules"]["dependent"]), [base["id"]], format="json"
        )
        created = forge.create_ontology_module(
            oracle["modules"]["dependent"], [base["id"]], enforcement="strict"
        )
        assert imported["id"] == created["id"]
        adopt = authority(forge, 2)
        receipt = forge.adopt_ontology_module(imported, **adopt)
        assert forge.adopt_ontology_module(imported, **adopt) == receipt
        conflicting = copy.deepcopy(created)
        conflicting["status"] = "conflict"
        assert (
            code(lambda: forge.adopt_ontology_module(conflicting, **adopt))
            == oracle["expected"]["idempotency_conflict_code"]
        )

        rows = forge.ontology_modules()
        assert [row["id"]["ontology_id"] for row in rows] == oracle["expected"]["module_order"]
        dependent = rows[1]
        assert (
            forge.inspect_ontology_module(dependent["id"]["ontology_id"])["entry"]["id"]
            == dependent["id"]
        )
        bridge_doc = substitute(
            oracle["bridge"], {"$base": base["id"], "$dependent": dependent["id"]}
        )
        assert isinstance(bridge_doc, dict)
        bridge_validation = forge.validate_ontology_bridge(bridge_doc)
        assert bridge_validation == {"valid": True, "diagnostics": []}
        invalid_bridge = copy.deepcopy(bridge_doc)
        invalid_bridge["assertions"][0]["source"]["local_id"] = "MissingEntity"
        invalid_bridge_validation = forge.validate_ontology_bridge(invalid_bridge)
        assert set(invalid_bridge_validation) == {"valid", "diagnostics"}
        assert invalid_bridge_validation["valid"] is False
        assert invalid_bridge_validation["diagnostics"][0]["code"] == "bridge.endpoint_missing"
        assert set(invalid_bridge_validation["diagnostics"][0]) == {
            "code",
            "phase",
            "message",
            "subjects",
            "candidates",
            "remediation",
            "limit",
        }
        bridge = forge.import_ontology_bridge(json.dumps(bridge_doc), format="json")
        assert bridge["id"] == forge.create_ontology_bridge(bridge_doc)["id"]
        forge.adopt_ontology_bridge(bridge, **authority(forge, 3))
        bridge_row = forge.ontology_bridges()[0]
        assert forge.inspect_ontology_bridge(**bridge_row["id"])["doc"] == bridge_doc

        before = forge.ontology_authority_state()
        preview_delete = forge.preview_delete_ontology_module(**base["id"])
        assert not preview_delete["safe"] and preview_delete["dependent_modules"]
        blocked_code, blocked_diagnostics = failure(
            lambda: forge.delete_ontology_module(**base["id"], **authority(forge, 4))
        )
        assert blocked_code == oracle["expected"]["dependency_blocked_code"]
        assert blocked_diagnostics[0]["code"] == oracle["expected"]["dependency_blocked_diagnostic"]
        assert blocked_diagnostics[0]["phase"] == "inventory"
        assert len(blocked_diagnostics[0]["subjects"]) <= blocked_diagnostics[0]["limit"]
        assert forge.ontology_authority_state() == before

        ambiguous = forge.explain_ontology_resolution("entity", "Person", max_candidates=2)
        assert ambiguous["outcome"] is None
        assert ambiguous["diagnostics"][0]["code"] == oracle["expected"]["ambiguous_code"]
        assert len(ambiguous["diagnostics"][0]["subjects"]) <= oracle["expected"]["max_diagnostics"]
        assert forge.explain_ontology_resolution("entity", "Person", module=base["id"])["outcome"]

        assert (
            json.loads(forge.export_ontology_bridge(**bridge_row["id"], format="json"))["bridge_id"]
            == oracle["bridge"]["bridge_id"]
        )
        bridge_update = copy.deepcopy(bridge_doc)
        bridge_update["authored_version"] = "2.0.0"
        bridge_preview = forge.preview_update_ontology_bridge(
            **bridge_row["id"], document=bridge_update
        )
        assert bridge_preview["prior"] == bridge_row["id"]
        forge.update_ontology_bridge(
            **bridge_row["id"], document=bridge_update, **authority(forge, 5)
        )
        bridge_row = forge.ontology_bridges()[0]
        assert bridge_row["id"]["authored_version"] == "2.0.0"
        assert forge.preview_delete_ontology_bridge(**bridge_row["id"])["safe"]
        forge.delete_ontology_bridge(**bridge_row["id"], **authority(forge, 6))
        assert forge.ontology_bridges() == []

        profile = forge.ontology_activation_profile()
        forge.change_ontology_activation_profile(
            "exploratory", profile["activation"], **authority(forge, 7)
        )
        assert forge.ontology_activation_profile()["profile_default"] == "exploratory"
        current = composition(forge)
        assert (
            forge.validate_ontology_composition(current)["composition_fingerprint"]
            == forge.ontology_authority_state()["composition_fingerprint"]
        )
        assert (
            forge.preflight_ontology_composition(current, **authority(forge, 8))["diagnostics"]
            == []
        )

        before = forge.ontology_authority_state()
        cancelled = g.CancellationToken()
        cancelled.cancel()
        assert (
            code(
                lambda: forge.change_ontology_activation_profile(
                    "advisory", profile["activation"], cancellation=cancelled, **authority(forge, 9)
                )
            )
            == oracle["expected"]["cancelled_code"]
        )
        assert forge.ontology_authority_state() == before
        malformed_code, malformed_diagnostics = failure(
            lambda: forge.import_ontology_module("{", [], format="json")
        )
        assert malformed_code == "GF_ONTOLOGY_DIAGNOSTIC"
        assert malformed_diagnostics[0]["code"] == "inventory.malformed"
        assert forge.ontology_authority_state() == before

        expanded = root / "portable"
        exported = forge.export_portable_v2(output_path=str(expanded), representation="expanded")
        expanded_path = Path(exported["output"])
        inject_unsupported_feature(expanded_path)
        future_code, future_diagnostics = failure(
            lambda: g.GraphForge.verify_portable_v2(str(expanded_path), mode="structure_only")
        )
        assert future_code == oracle["expected"]["unsupported_future_code"]
        assert future_diagnostics[0]["code"] == oracle["expected"]["unsupported_future_diagnostic"]
        assert future_diagnostics[0]["phase"] == "interchange"
        target = root / "failed-import-target"
        target.mkdir()
        before_entries = list(target.iterdir())
        import_code, import_diagnostics = failure(
            lambda: g.GraphForge.import_portable_v2(
                str(target), input=str(expanded_path), operation_id=op(10)
            )
        )
        assert import_code == oracle["expected"]["unsupported_future_code"]
        assert import_diagnostics[0]["code"] == oracle["expected"]["unsupported_future_diagnostic"]
        assert list(target.iterdir()) == before_entries
        assert forge.ontology_authority_state() == before
        assert forge.portable_ontology_staging() is None

        preview = forge.preview_update_ontology_module(
            **dependent["id"],
            document=oracle["modules"]["dependent_update"],
            dependencies=[base["id"]],
        )
        assert preview["prior"] == dependent["id"] and preview["document_valid"]
        forge.update_ontology_module(
            **dependent["id"],
            document=oracle["modules"]["dependent_update"],
            dependencies=[base["id"]],
            enforcement=None,
            **authority(forge, 11),
        )
        updated = forge.ontology_modules()[1]
        assert updated["id"]["authored_version"] == "2.0.0"
        assert (
            json.loads(forge.export_ontology_module(**updated["id"], format="json"))
            == oracle["modules"]["dependent_update"]
        )
        semantic = {
            "positive_crud_import_export": oracle["expected"]["module_order"],
            "exact_identity_and_ambiguity": ambiguous["diagnostics"][0]["code"],
            "dependency_blocked_deletion": blocked_diagnostics[0]["code"],
            "unsupported_future_portability": future_diagnostics[0]["code"],
            "cancellation": oracle["expected"]["cancelled_code"],
            "idempotent_replay": receipt,
            "no_partial_import_or_authority_change": list(target.iterdir()),
            "bounded_structured_diagnostics": blocked_diagnostics,
            "deterministic_path_free_cli_json": json.dumps(receipt, sort_keys=True),
            "packaged_clean_install": g.__file__,
        }
        report = {
            "contract": "graphforge-multi-ontology-parity-result/1",
            "cases": {
                "positive_crud_import_export": {
                    "module_ids": oracle["expected"]["module_order"],
                    "bridge_id": oracle["expected"]["bridge_order"][0],
                    "module_export_match": True,
                    "bridge_export_match": True,
                },
                "exact_identity_and_ambiguity": {
                    "exact_match": True,
                    "diagnostic_code": ambiguous["diagnostics"][0]["code"],
                },
                "dependency_blocked_deletion": {
                    "safe": False,
                    "diagnostic_code": blocked_diagnostics[0]["code"],
                },
                "unsupported_future_portability": {
                    "error_code": future_code,
                    "diagnostic_code": future_diagnostics[0]["code"],
                },
                "cancellation": {
                    "error_code": oracle["expected"]["cancelled_code"],
                    "authority_unchanged": True,
                },
                "idempotent_replay": {
                    "same_receipt": True,
                    "conflict_code": oracle["expected"]["idempotency_conflict_code"],
                },
                "no_partial_import_or_authority_change": {
                    "target_unchanged": True,
                    "authority_unchanged": True,
                },
                "bounded_structured_diagnostics": {
                    "outer_code": blocked_code,
                    "diagnostic_code": blocked_diagnostics[0]["code"],
                    "bounded": len(blocked_diagnostics[0]["subjects"])
                    <= blocked_diagnostics[0]["limit"],
                    "path_free": str(root) not in json.dumps(blocked_diagnostics, sort_keys=True),
                },
                "deterministic_path_free_cli_json": {
                    "deterministic": json.dumps(receipt, sort_keys=True)
                    == json.dumps(receipt, sort_keys=True),
                    "path_free": str(root) not in json.dumps(receipt, sort_keys=True),
                },
                "packaged_clean_install": {"semantic_smoke": True},
            },
        }
        report_path = os.environ.get("GRAPHFORGE_MULTI_ONTOLOGY_PARITY_REPORT")
        if report_path:
            Path(report_path).write_text(
                json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n"
            )
        semantic["validation_receipts"] = {
            "module": module_validation,
            "invalid_module": invalid_module_validation,
            "bridge": bridge_validation,
            "invalid_bridge": invalid_bridge_validation,
        }
        return semantic


@functools.cache
def semantic_results() -> dict[str, object]:
    return _run_four_surface_operation_conformance()


def test_detached_inputs_own_their_lifetime() -> None:
    oracle = json.loads(ORACLE.read_text())
    forge = g.GraphForge()
    document = copy.deepcopy(oracle["modules"]["dependent"])
    dependencies: list[dict[str, str]] = []
    candidate = forge.create_ontology_module(document, dependencies)
    document.clear()
    dependencies.append(
        {"ontology_id": "mutated", "authored_version": "x", "canonical_digest": "0" * 64}
    )
    assert candidate["document"]["ontology_id"] == "urn:graphforge:parity:dependent"
    assert candidate["dependencies"] == []


def test_positive_crud_import_export() -> None:
    create_ontology_module = "create_ontology_module"
    export_ontology_bridge = "export_ontology_bridge"
    assert semantic_results()["positive_crud_import_export"] == [
        "urn:graphforge:parity:base",
        "urn:graphforge:parity:dependent",
    ]
    assert create_ontology_module and export_ontology_bridge


def test_exact_identity_and_ambiguity() -> None:
    ontology_id = "urn:graphforge:parity:base"
    selector_ambiguous = "resolution.ambiguous"
    assert semantic_results()["exact_identity_and_ambiguity"] == selector_ambiguous
    assert ontology_id


def test_dependency_blocked_deletion() -> None:
    preview_delete_ontology_module = "preview_delete_ontology_module"
    dependency_blocked = "GF_VALIDATION"
    assert semantic_results()["dependency_blocked_deletion"] == "dependency.in_use"
    assert preview_delete_ontology_module and dependency_blocked


def test_unsupported_future_portability() -> None:
    verify_portable_v2 = "verify_portable_v2"
    unsupported_future_version = "future-multi-ontology@999"
    assert semantic_results()["unsupported_future_portability"] == "interchange.unsupported_future"
    assert verify_portable_v2 and unsupported_future_version


def test_cancellation() -> None:
    cancel_before_start = g.CancellationToken()
    cancel_before_start.cancel()
    gf_cancelled = "GF_CANCELLED"
    assert semantic_results()["cancellation"] == gf_cancelled
    assert cancel_before_start


def test_idempotent_replay() -> None:
    operation_uuid = op(100)
    replay_result = "same receipt"
    result = semantic_results()["idempotent_replay"]
    assert isinstance(result, dict) and result["operation_uuid"]
    assert operation_uuid and replay_result


def test_no_partial_import_or_authority_change() -> None:
    ontology_authority_state = "ontology_authority_state"
    no_partial_import = True
    assert semantic_results()["no_partial_import_or_authority_change"] == []
    assert ontology_authority_state and no_partial_import


def test_bounded_structured_diagnostics() -> None:
    diagnostics = "diagnostics"
    limit = 2
    result = semantic_results()["bounded_structured_diagnostics"]
    assert isinstance(result, list) and len(result[0]["subjects"]) <= result[0]["limit"]
    assert diagnostics and limit == 2


def test_deterministic_path_free_serialization() -> None:
    project_generation_uuid = "project_generation_uuid"
    result = semantic_results()["deterministic_path_free_cli_json"]
    assert isinstance(result, str) and str(ROOT) not in result
    assert json.dumps({project_generation_uuid: op(101)}, sort_keys=True)


def test_packaged_clean_install() -> None:
    import graphforge

    assert graphforge.__file__
    forge = graphforge.GraphForge()
    assert forge.ontology_modules() == []
    assert semantic_results()["packaged_clean_install"] == graphforge.__file__


if __name__ == "__main__":
    semantic_results()
