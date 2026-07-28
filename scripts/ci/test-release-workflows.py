#!/usr/bin/env python3
"""Mutation tests for the release-workflow registry and orchestrator."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).with_name("release-workflows.py")
SPEC = importlib.util.spec_from_file_location("release_workflows", SCRIPT)
assert SPEC and SPEC.loader
release_workflows = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_workflows)


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ids: tuple[str, ...] = ("alpha", "beta")
) -> tuple[Path, dict]:
    root = tmp_path
    workflows = root / "tests/release_workflows"
    taxonomy = workflows / "ontology-complexity-v1.json"
    write_json(
        taxonomy,
        {
            "schema": "ontology-complexity-v1",
            "formula": {"kind": "weighted-sum", "weights": {"entity_types": 1}},
            "states": ["strict"],
            "classes": {"small": {"minimum": 0, "maximum": 9}},
        },
    )
    write_json(workflows / "registry-v1.schema.json", {})
    write_json(workflows / "evidence-envelope-v1.schema.json", {})
    rows = []
    for position, scenario_id in enumerate(ids):
        bundle = workflows / scenario_id
        bundle.mkdir(parents=True)
        generator = bundle / "generator.yaml"
        generator.write_text('{"seed": 1}\n', encoding="utf-8")
        digest = release_workflows.hashlib.sha256(generator.read_bytes()).hexdigest()
        steps = [f"{scenario_id.upper()}-01", f"{scenario_id.upper()}-02"]
        write_json(
            bundle / "scenario.yaml",
            {
                "registry": {
                    "schema": "workflow-scenario-v1",
                    "id": scenario_id,
                    "steps": steps,
                    "generator_fingerprint": f"sha256:{digest}",
                    "ontology_profile": "small",
                    "evidence": f"target/{scenario_id}.json",
                }
            },
        )
        (bundle / "workflow.feature").write_text(
            f"Feature: {scenario_id}\n"
            "  Scenario: test\n"
            f"    Given [{steps[0]}] first\n"
            f"    Then [{steps[1]}] second\n",
            encoding="utf-8",
        )
        (bundle / "README.md").write_text("fixture\n", encoding="utf-8")
        write_json(bundle / "expected/arrow-fingerprints.json", {})
        write_json(bundle / "expected/errors.json", {})
        runner = bundle / "run.py"
        runner.write_text(
            "from pathlib import Path\n"
            "import json,sys\n"
            "sha=sys.argv[-2]\n"
            "p=Path(sys.argv[-1])\n"
            "p.parent.mkdir(parents=True,exist_ok=True)\n"
            "p.write_text(json.dumps({'ok':True,'commit_sha':sha}))\n",
            encoding="utf-8",
        )
        implementation = root / f"implementation-{scenario_id}.txt"
        implementation.write_text("implemented\n", encoding="utf-8")
        rows.append(
            {
                "id": scenario_id,
                "title": scenario_id,
                "version": "1",
                "domain": "test",
                "owning_issue": position + 1,
                "bundle": f"tests/release_workflows/{scenario_id}",
                "implementation": implementation.name,
                "evidence": f"target/{scenario_id}.json",
                "steps": steps,
                "generator": {
                    "path": f"tests/release_workflows/{scenario_id}/generator.yaml",
                    "seed": 1,
                    "sha256": digest,
                },
                "public_surfaces": ["execute"],
                "m18_m19": [],
                "m20_m21": [],
                "axes": {
                    "correction": "none",
                    "temporal": "none",
                    "epistemic": "none",
                    "binding": "rust",
                },
                "coverage_signature": (
                    f"load-{scenario_id}|shape-{scenario_id}|chain-{scenario_id}|"
                    "correction|temporal|binding"
                ),
                "ontology_profile": "small",
                "command": [
                    "python3",
                    f"tests/release_workflows/{scenario_id}/run.py",
                    "{sha}",
                    "{evidence}",
                ],
                "resource": "small",
                "timeout_seconds": 5,
            }
        )
    registry = workflows / "registry-v1.json"
    data = {
        "schema": "workflow-registry-v1",
        "taxonomy": "ontology-complexity-v1",
        "evidence_schema": "evidence-envelope-v1",
        "scenarios": rows,
    }
    write_json(registry, data)
    monkeypatch.setattr(release_workflows, "ROOT", root)
    monkeypatch.setattr(release_workflows, "WORKFLOWS", workflows)
    monkeypatch.setattr(release_workflows, "REGISTRY", registry)
    monkeypatch.setattr(release_workflows, "TAXONOMY", taxonomy)
    return registry, data


def test_valid_registry_and_selected_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry, _ = fixture(tmp_path, monkeypatch)
    rows = release_workflows.validate_registry(registry)
    assert [row["id"] for row in release_workflows.select(rows, ["beta", "alpha"], False)] == [
        "alpha",
        "beta",
    ]


def test_near_duplicate_requires_release_risk_rationale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry, data = fixture(tmp_path, monkeypatch)
    data["scenarios"][1]["coverage_signature"] = data["scenarios"][0]["coverage_signature"].replace(
        "shape-alpha", "shape-beta"
    )
    write_json(registry, data)
    with pytest.raises(release_workflows.ContractError, match="near-duplicate"):
        release_workflows.validate_registry(registry)
    data["scenarios"][1]["release_risk_rationale"] = "Exercises a distinct release risk."
    write_json(registry, data)
    assert len(release_workflows.validate_registry(registry)) == 2


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "unregistered",
        "identity",
        "steps",
        "fingerprint",
        "taxonomy",
        "signature",
        "traversal",
    ],
)
def test_validator_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    registry, data = fixture(tmp_path, monkeypatch)
    if mutation == "missing":
        (release_workflows.WORKFLOWS / "alpha/README.md").unlink()
    elif mutation == "unregistered":
        extra = release_workflows.WORKFLOWS / "extra"
        extra.mkdir()
        write_json(extra / "scenario.yaml", {})
    elif mutation == "identity":
        manifest = release_workflows.load_json(release_workflows.WORKFLOWS / "alpha/scenario.yaml")
        manifest["registry"]["id"] = "wrong"
        write_json(release_workflows.WORKFLOWS / "alpha/scenario.yaml", manifest)
    elif mutation == "steps":
        data["scenarios"][0]["steps"].reverse()
        write_json(registry, data)
    elif mutation == "fingerprint":
        data["scenarios"][0]["generator"]["sha256"] = "0" * 64
        write_json(registry, data)
    elif mutation == "taxonomy":
        data["scenarios"][0]["ontology_profile"] = "unknown"
        write_json(registry, data)
    elif mutation == "signature":
        data["scenarios"][1]["coverage_signature"] = data["scenarios"][0]["coverage_signature"]
        write_json(registry, data)
    else:
        data["scenarios"][0]["evidence"] = "../escape.json"
        write_json(registry, data)
    with pytest.raises(release_workflows.ContractError):
        release_workflows.validate_registry(registry)


def test_lightweight_all_execution_is_registry_ordered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry, _ = fixture(tmp_path, monkeypatch)
    rows = release_workflows.validate_registry(registry)
    output = tmp_path / "target/release-workflow-evidence/envelope.json"
    assert release_workflows.run(rows, "a" * 40, output) == 0
    envelope = release_workflows.load_json(output)
    assert envelope["selected_scenarios"] == ["alpha", "beta"]
    assert [child["scenario_id"] for child in envelope["children"]] == ["alpha", "beta"]
    release_workflows.validate_evidence(output, "a" * 40)


def test_wrong_sha_and_stale_child_evidence_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry, _ = fixture(tmp_path, monkeypatch, ("alpha",))
    rows = release_workflows.validate_registry(registry)
    output = tmp_path / "target/release-workflow-evidence/envelope.json"
    assert release_workflows.run(rows, "b" * 40, output) == 0
    with pytest.raises(release_workflows.ContractError, match="SHA"):
        release_workflows.validate_evidence(output, "c" * 40)
    (tmp_path / "target/alpha.json").write_text("changed", encoding="utf-8")
    with pytest.raises(release_workflows.ContractError, match="stale"):
        release_workflows.validate_evidence(output, "b" * 40)


def test_stale_evidence_is_removed_before_child_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry, _ = fixture(tmp_path, monkeypatch, ("alpha",))
    rows = release_workflows.validate_registry(registry)
    evidence = tmp_path / "target/alpha.json"
    write_json(evidence, {"commit_sha": "d" * 40})
    (tmp_path / "tests/release_workflows/alpha/run.py").write_text(
        "# intentionally produces no evidence\n", encoding="utf-8"
    )
    output = tmp_path / "target/release-workflow-evidence/envelope.json"
    assert release_workflows.run(rows, "d" * 40, output) == 1
    assert not evidence.exists()
    envelope = release_workflows.load_json(output)
    assert envelope["children"][0]["outcome"] == "failed"


def test_timeout_is_attributable_and_all_children_are_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry, data = fixture(tmp_path, monkeypatch)
    (tmp_path / "tests/release_workflows/alpha/run.py").write_text(
        "import time\ntime.sleep(1)\n", encoding="utf-8"
    )
    data["scenarios"][0]["timeout_seconds"] = 0.01
    write_json(registry, data)
    rows = release_workflows.validate_registry(registry)
    output = tmp_path / "target/release-workflow-evidence/envelope.json"
    assert release_workflows.run(rows, "e" * 40, output) == 1
    envelope = release_workflows.load_json(output)
    assert [child["scenario_id"] for child in envelope["children"]] == ["alpha", "beta"]
    assert envelope["children"][0]["outcome"] == "failed"
    assert envelope["children"][1]["outcome"] == "passed"
    release_workflows.validate_evidence(output, "e" * 40)


def test_output_must_stay_in_evidence_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry, _ = fixture(tmp_path, monkeypatch, ("alpha",))
    rows = release_workflows.validate_registry(registry)
    with pytest.raises(release_workflows.ContractError, match="output"):
        release_workflows.run(rows, "f" * 40, tmp_path / "outside.json")
