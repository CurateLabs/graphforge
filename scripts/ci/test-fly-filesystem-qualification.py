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


controller = load(ROOT / "scripts/fly-filesystem-qualification.py", "fly_controller")
validator = load(ROOT / "scripts/ci/validate-fly-filesystem-qualification.py", "fly_validator")


def args(**changes):
    values = {
        "expected_sha": "a" * 40,
        "image": "registry.example/graphforge@sha256:" + "b" * 64,
        "region": "den",
        "org": "curate",
        "app_name": "gf-qual-app",
        "volume_name": "gf-qual-vol",
        "machine_name": "gf-qual-machine",
        "cpus": 2,
        "memory_mb": 4096,
        "volume_size_gb": 10,
        "retrieve_timeout_s": 60,
        "evidence_out": Path("unused.json"),
        "execute": False,
        "confirm_disposable": False,
    }
    values.update(changes)
    return argparse.Namespace(**values)


def evidence(**changes):
    value = {
        "schema": "graphforge-fly-filesystem-qualification/1",
        "git_sha": "a" * 40,
        "image_digest": "sha256:" + "b" * 64,
        "provider": "fly.io",
        "region": "den",
        "host": {"os": "Linux", "filesystem": "ext4", "memory_bytes": 4_000_000_000},
        "volume": {"mount_role": "process_work_root", "capacity_bytes": 10_000_000_000},
        "admission": {"status": "accepted", "code": None, "cause": None},
        "result": "qualified",
        "full_run_authorized": False,
    }
    value.update(changes)
    return value


def test_refuses_mutable_image_and_execute_without_confirmation():
    with pytest.raises(controller.QualificationError, match="immutable"):
        controller.validate_inputs(args(image="registry.example/graphforge:latest"))
    with pytest.raises(controller.QualificationError, match="confirm-disposable"):
        controller.validate_inputs(args(execute=True))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"cpus": 0}, "CPU count"),
        ({"memory_mb": 131073}, "memory"),
        ({"retrieve_timeout_s": 59}, "retrieval timeout"),
    ],
)
def test_refuses_out_of_bounds_machine_and_timeout_values(change, message):
    with pytest.raises(controller.QualificationError, match=message):
        controller.validate_inputs(args(**change))


def test_refuses_dirty_or_non_exact_source(monkeypatch):
    responses = iter(
        [
            argparse.Namespace(stdout="b" * 40 + "\n"),
            argparse.Namespace(stdout=""),
        ]
    )
    monkeypatch.setattr(
        controller.subprocess, "run", lambda *_args, **_kwargs: next(responses)
    )
    with pytest.raises(controller.QualificationError, match="not the checked-out HEAD"):
        controller.check_source("a" * 40)

    responses = iter(
        [
            argparse.Namespace(stdout="a" * 40 + "\n"),
            argparse.Namespace(stdout="?? unexpected\n"),
        ]
    )
    with pytest.raises(controller.QualificationError, match="not clean"):
        controller.check_source("a" * 40)


def test_launch_has_private_disposable_exact_resources():
    command = controller.launch_args(args(), "internal-volume-id")
    assert command[2].endswith("@sha256:" + "b" * 64)
    assert [
        command[command.index(flag) + 1]
        for flag in ("--region", "--restart", "--autostop", "--vm-cpu-kind", "--vm-memory")
    ] == ["den", "no", "off", "performance", "4096"]
    assert "--rm" in command and "--skip-dns-registration" in command
    assert not any(flag in command for flag in ("--port", "--http-service", "--public-ip"))


def test_observed_config_rejects_service_or_wrong_mount():
    machine = {
        "region": "den",
        "image_ref": {"digest": "sha256:" + "b" * 64},
        "config": {
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "guest": {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096},
            "mounts": [{"path": "/work"}],
            "services": [],
        },
    }
    controller.assert_machine_config(machine, args(), "sha256:" + "b" * 64)
    machine["config"]["services"] = [{"ports": [443]}]
    with pytest.raises(controller.QualificationError, match="service"):
        controller.assert_machine_config(machine, args(), "sha256:" + "b" * 64)


def test_validator_accepts_sanitized_evidence_and_rejects_leakage():
    validator.validate(evidence(), sha="a" * 40, digest="sha256:" + "b" * 64, region="den")
    leaked = evidence(machine_id="9080deadbeef")
    with pytest.raises(validator.EvidenceError, match="schema violation"):
        validator.validate(leaked, sha="a" * 40, digest="sha256:" + "b" * 64, region="den")
    leaked = evidence()
    leaked["host"]["filesystem"] = "/dev/vdc"
    with pytest.raises(validator.EvidenceError):
        validator.validate(leaked, sha="a" * 40, digest="sha256:" + "b" * 64, region="den")


def test_rejection_is_typed_and_never_authorizes_full_run():
    rejected = evidence(
        admission={
            "status": "rejected",
            "code": "GF_UNSUPPORTED_FILESYSTEM",
            "cause": "filesystem_class_unproven",
        },
        result="disqualified",
    )
    validator.validate(rejected, sha="a" * 40, digest="sha256:" + "b" * 64, region="den")
    rejected["full_run_authorized"] = True
    with pytest.raises(validator.EvidenceError):
        validator.validate(rejected, sha="a" * 40, digest="sha256:" + "b" * 64, region="den")


class FakeFly:
    def __init__(self):
        self.calls = []

    def run(self, command, check=True):
        self.calls.append(command)
        return argparse.Namespace(returncode=0, stdout="", stderr="")


def test_teardown_is_child_first_and_idempotent():
    fake = FakeFly()
    controller.cleanup(fake, "gf-qual-app", "machine-internal", "volume-internal")
    controller.cleanup(fake, "gf-qual-app", None, None)
    assert [call[:2] for call in fake.calls[:3]] == [
        ["machine", "destroy"],
        ["volumes", "destroy"],
        ["apps", "destroy"],
    ]
    assert fake.calls[-1][:2] == ["apps", "destroy"]


def test_schema_is_closed_and_committed():
    schema = json.loads(
        (ROOT / "docs/development/evidence/fly-filesystem-qualification.schema.json").read_text()
    )
    assert schema["additionalProperties"] is False
    assert schema["properties"]["host"]["additionalProperties"] is False
