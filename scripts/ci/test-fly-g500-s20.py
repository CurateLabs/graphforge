#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


controller = load(ROOT / "scripts/fly-g500-s20.py", "fly_s20_controller")
validator = load(ROOT / "scripts/ci/validate-fly-g500-s20.py", "fly_s20_validator")


def args(tmp_path: Path, **changes):
    values = {
        "expected_sha": "a" * 40,
        "image": "registry.example/graphforge@sha256:" + "b" * 64,
        "region": "den",
        "org": "curatelabs",
        "app_name": "gf-s20-unique",
        "volume_name": "gf_s20_unique",
        "machine_name": "gf-s20-machine",
        "volume_size_gb": 500,
        "timeout_s": 14_400,
        "reserved_cost_usd": 5.0,
        "ledger": tmp_path / "ledger.json",
        "evidence_out": tmp_path / "evidence.json",
        "execute": False,
        "confirm_disposable": False,
    }
    values.update(changes)
    return argparse.Namespace(**values)


def evidence(**changes):
    value = {
        "schema": "graphforge-fly-g500-s20/1",
        "git_sha": "a" * 40,
        "image_digest": "sha256:" + "b" * 64,
        "provider": "fly.io",
        "region": "den",
        "scale": 20,
        "result": "passed",
        "counts": {
            "generated_edges": 16_777_216,
            "source_edges": 16_777_216,
            "imported_edges": 16_777_216,
        },
        "phase_peak_rss_bytes": {
            "generate": 500_000_000,
            "ingest": 800_000_000,
            "source_reopen": 700_000_000,
            "one_hop": 900_000_000,
            "two_hop": 950_000_000,
            "export": 700_000_000,
            "verify": 600_000_000,
            "import": 850_000_000,
            "import_reopen": 700_000_000,
        },
        "storage": {
            "logical_bytes": 10_000,
            "allocated_bytes": 12_288,
            "capacity_bytes": 500_000_000_000,
        },
    }
    value.update(changes)
    return value


def test_contract_fixes_resources_and_rejects_mutable_image(tmp_path):
    digest = controller.validate_inputs(args(tmp_path))
    payload = controller.machine_payload(args(tmp_path), "vol-id", digest)
    assert payload["config"]["guest"] == {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096}
    assert payload["config"]["services"] == []
    assert payload["config"]["restart"] == {"policy": "no"}
    assert payload["config"]["mounts"] == [{"volume": "vol-id", "path": "/work"}]
    assert payload["config"]["env"]["GF_G500_CERTIFICATION_SCALE"] == "20"
    with pytest.raises(controller.ControllerError, match="immutable"):
        controller.validate_inputs(args(tmp_path, image="registry.example/graphforge:latest"))


@pytest.mark.parametrize("size", [0, 501])
def test_volume_is_explicitly_bounded_by_fly_limit(tmp_path, size):
    with pytest.raises(controller.ControllerError, match=r"1\.\.500"):
        controller.validate_inputs(args(tmp_path, volume_size_gb=size))


def test_execute_requires_confirmation_and_cost_never_exceeds_ten(tmp_path):
    with pytest.raises(controller.ControllerError, match="confirm-disposable"):
        controller.validate_inputs(args(tmp_path, execute=True))
    with pytest.raises(controller.ControllerError, match=r"\$10"):
        controller.validate_inputs(args(tmp_path, reserved_cost_usd=10.01))


def test_durable_budget_reservations_survive_and_accumulate(tmp_path):
    ledger = tmp_path / "ledger.json"
    controller.reserve_budget(ledger, "run-one", 6.0)
    controller.reserve_budget(ledger, "run-two", 4.0)
    state = json.loads(ledger.read_text())
    assert sum(run["reserved_usd"] for run in state["runs"]) == 10.0
    with pytest.raises(controller.ControllerError, match="exceed"):
        controller.reserve_budget(ledger, "run-three", 0.01)
    with pytest.raises(controller.ControllerError, match="already reserved"):
        controller.reserve_budget(ledger, "run-one", 1.0)


def test_existing_app_is_refused_before_budget_or_creation(tmp_path):
    class ExistingFly:
        def json(self, command):
            assert command == ["apps", "list"]
            return [{"name": "gf-s20-unique"}]

        def run(self, *_args, **_kwargs):
            raise AssertionError("must not mutate provider state")

    run = args(tmp_path, execute=True, confirm_disposable=True)
    with pytest.raises(controller.ControllerError, match="existing app"):
        controller.execute(run, ExistingFly(), "sha256:" + "b" * 64)
    assert not run.ledger.exists()


def test_cleanup_only_uses_observed_owned_identifiers():
    calls = []

    class Fly:
        def run(self, command, check=True):
            calls.append((command, check))

    controller.cleanup_owned(Fly(), "gf-s20-unique", None, None, False)
    assert calls == []
    controller.cleanup_owned(Fly(), "gf-s20-unique", "machine-observed", "volume-observed", True)
    assert [call[0][0:2] for call in calls] == [
        ["machine", "destroy"],
        ["volumes", "destroy"],
        ["apps", "destroy"],
    ]


def test_observed_machine_must_match_private_fixed_plan(tmp_path):
    digest = "sha256:" + "b" * 64
    machine = {
        "region": "den",
        "image_ref": {"digest": digest},
        "config": {
            "guest": {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096},
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "services": [],
            "mounts": [{"path": "/work"}],
        },
    }
    controller.assert_machine(machine, args(tmp_path), digest)
    machine["config"]["services"] = [{"ports": [443]}]
    with pytest.raises(controller.ControllerError, match="service"):
        controller.assert_machine(machine, args(tmp_path), digest)


def test_closed_evidence_accepts_only_pinned_sanitized_s20():
    validator.validate(evidence(), "a" * 40, "sha256:" + "b" * 64, "den")
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(
            evidence(machine_id="provider-secret-id"), "a" * 40, "sha256:" + "b" * 64, "den"
        )
    with pytest.raises(validator.EvidenceError, match="identity"):
        validator.validate(evidence(region="ord"), "a" * 40, "sha256:" + "b" * 64, "den")


def test_no_lower_rung_or_dynamic_sizing_contract_exists():
    source = (ROOT / "scripts/fly-g500-s20.py").read_text()
    for forbidden in ("S18", "S19", "lower_rung", "rss_ratio", "dynamic_memory", "S26"):
        assert forbidden not in source
