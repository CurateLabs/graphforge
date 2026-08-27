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
        "ledger": tmp_path / "ledger.json",
        "evidence_out": tmp_path / "evidence.json",
        "diagnostic_out": tmp_path / "diagnostic.json",
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
        "machine": {"class": "performance", "cpus": 2, "memory_mb": 4096},
        "volume_gb": 500,
        "result": "passed",
        "counts": {
            "generated_edges": 16_777_216,
            "source_edges": 16_777_216,
            "imported_edges": 16_777_216,
        },
        "phase_memory": {
            phase: {
                "rss_peak_bytes": 500_000_000,
                "hwm_bytes": 600_000_000,
                "anonymous_peak_bytes": 300_000_000,
                "file_peak_bytes": 200_000_000,
                "sample_interval_ms": 250,
            }
            for phase in (
                "generate",
                "ingest",
                "source_reopen",
                "source_query",
                "export",
                "verify",
                "import",
                "import_reopen",
                "import_query",
                "finalize",
            )
        },
        "storage": {
            "logical_bytes": 10_000,
            "allocated_bytes": 12_288,
            "peak_allocated_bytes": 16_384,
            "generator_allocated_bytes": 4_096,
            "construction_transient_peak_allocated_bytes": 4_096,
            "capacity_bytes": 500_000_000_000,
        },
        "run": {"scale": 20, "edgefactor": 16, "seed": 1},
        "rung": {"pass": True},
        "lifecycle": {"source_edges": 16_777_216, "imported_edges": 16_777_216},
        "memory": {"rss_bytes": 1},
        "wall_time_s": 1.0,
        "first_failure": None,
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
    assert payload["config"]["env"]["GF_G500_S20_EXPECTED_SHA"] == "a" * 40
    assert payload["config"]["env"]["GF_G500_S20_VOLUME_GB"] == "500"
    assert payload["config"]["env"]["GF_G500_S20_EVIDENCE_OUT"] == "/work/s20-evidence.json"
    assert payload["config"]["env"]["GF_G500_S20_RESULT_OUT"] == "/work/container-result.json"
    assert payload["config"]["env"]["GF_G500_S20_TIMEOUT_SECONDS"] == "14400"
    with pytest.raises(controller.ControllerError, match="immutable"):
        controller.validate_inputs(args(tmp_path, image="registry.example/graphforge:latest"))


@pytest.mark.parametrize("size", [0, 501])
def test_volume_is_explicitly_bounded_by_fly_limit(tmp_path, size):
    with pytest.raises(controller.ControllerError, match=r"1\.\.500"):
        controller.validate_inputs(args(tmp_path, volume_size_gb=size))


def test_execute_requires_confirmation(tmp_path):
    with pytest.raises(controller.ControllerError, match="confirm-disposable"):
        controller.validate_inputs(args(tmp_path, execute=True))


def test_durable_budget_reservations_survive_and_accumulate(tmp_path):
    ledger = tmp_path / "ledger.json"
    first = controller.price_reservation(500)
    assert first["reserved_usd"] == 1.11
    assert first["runtime_seconds"] == 14_400
    assert first["cleanup_reserve_seconds"] == 600
    assert first["volume_billing_hours"] == 5
    controller.reserve_budget(ledger, "run-one", first)
    controller.reserve_budget(ledger, "run-two", first)
    state = json.loads(ledger.read_text())
    assert sum(run["reserved_usd"] for run in state["runs"]) == 2.22
    assert state["runs"][0]["pricing_source"] == "https://fly.io/docs/about/pricing/"
    oversized = {**first, "reserved_usd": 8.0}
    with pytest.raises(controller.ControllerError, match="exceed"):
        controller.reserve_budget(ledger, "run-three", oversized)
    with pytest.raises(controller.ControllerError, match="already reserved"):
        controller.reserve_budget(ledger, "run-one", first)


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


def test_post_cleanup_absence_is_verified_child_to_parent():
    calls = []

    class Fly:
        def run(self, command, check=True):
            calls.append(command)
            return argparse.Namespace(returncode=1)

        def json(self, command):
            calls.append(command)
            return []

    controller.verify_absent(Fly(), "gf-s20-unique", "machine-observed", "volume-observed", True)
    assert calls == [
        ["machine", "status", "machine-observed", "--app", "gf-s20-unique"],
        ["volumes", "show", "volume-observed", "--app", "gf-s20-unique"],
        ["apps", "list"],
    ]


def test_post_cleanup_detects_each_surviving_owned_resource():
    class Fly:
        def __init__(self, survivor):
            self.survivor = survivor

        def run(self, command, check=True):
            return argparse.Namespace(returncode=0 if command[0] == self.survivor else 1)

        def json(self, _command):
            return [{"name": "gf-s20-unique"}] if self.survivor == "app" else []

    for survivor, message in (("machine", "Machine"), ("volumes", "volume"), ("app", "app")):
        with pytest.raises(controller.ControllerError, match=message):
            controller.verify_absent(
                Fly(survivor), "gf-s20-unique", "machine-observed", "volume-observed", True
            )


def test_container_result_is_closed_typed_and_sanitized():
    assert controller.validate_container_result({"status": "success"}) == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "success",
    }
    failure = controller.validate_container_result(
        {"status": "failure", "phase": "ingest", "code": "GF_RESOURCE_EXHAUSTED"}
    )
    assert failure["phase"] == "ingest"
    with pytest.raises(controller.ControllerError, match="unknown fields"):
        controller.validate_container_result(
            {"status": "failure", "phase": "ingest", "code": "failed", "path": "/secret"}
        )
    with pytest.raises(controller.ControllerError, match="invalid code"):
        controller.validate_container_result(
            {"status": "failure", "phase": "ingest", "code": "token=secret"}
        )


def test_fetch_binds_only_declared_runtime_paths(tmp_path):
    calls = []

    class Fly:
        def run(self, command, check=True):
            calls.append(command)
            return argparse.Namespace(returncode=1)

    run = args(tmp_path)
    controller.fetch(Fly(), run, "machine-observed", controller.RESULT_PATH, tmp_path / "r")
    controller.fetch(Fly(), run, "machine-observed", controller.EVIDENCE_PATH, tmp_path / "e")
    assert [call[3] for call in calls] == ["/work/container-result.json", "/work/s20-evidence.json"]


def test_terminal_failure_is_persisted_promptly_and_still_verified_cleaned(tmp_path, monkeypatch):
    digest = "sha256:" + "b" * 64
    machine = {
        "id": "machine-observed",
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
    monkeypatch.setattr(controller, "create_machine", lambda *_args: machine)

    class Fly:
        def __init__(self):
            self.app_lists = 0

        def json(self, command):
            if command[:2] == ["apps", "list"]:
                self.app_lists += 1
                return []
            if command[:2] == ["volumes", "create"]:
                return {"id": "volume-observed"}
            raise AssertionError(command)

        def run(self, command, check=True):
            if command[:3] == ["ssh", "sftp", "get"]:
                assert command[3] == "/work/container-result.json"
                Path(command[4]).write_text(
                    json.dumps({"status": "failure", "phase": "ingest", "code": "GF_OOM"})
                )
                return argparse.Namespace(returncode=0)
            return argparse.Namespace(returncode=1)

    run = args(tmp_path, execute=True, confirm_disposable=True)
    fly = Fly()
    with pytest.raises(controller.ControllerError, match="GF_OOM"):
        controller.execute(run, fly, digest)
    assert json.loads(run.diagnostic_out.read_text()) == {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": "ingest",
        "code": "GF_OOM",
    }
    assert fly.app_lists == 2


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
