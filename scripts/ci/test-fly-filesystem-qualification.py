#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess

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
        "volume_name": "gf_qual_vol",
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
        "phase_peak_rss_bytes": {
            "filesystem_admission": 100_000_000,
            "durable_reopen": 110_000_000,
            "portable_verify": 120_000_000,
            "portable_import_reopen": 125_000_000,
        },
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


@pytest.mark.parametrize("volume_name", ["v", "v" + "0" * 29, "gf_qual_vol"])
def test_accepts_fly_valid_volume_names(volume_name):
    controller.validate_inputs(args(volume_name=volume_name))


@pytest.mark.parametrize(
    "volume_name",
    ["gf-qual-vol", "GF_QUAL_VOL", "_gf_qual_vol", "v" + "0" * 30],
)
def test_refuses_fly_invalid_volume_names(volume_name):
    with pytest.raises(controller.QualificationError, match="volume name"):
        controller.validate_inputs(args(volume_name=volume_name))


def test_volume_create_command_uses_valid_name_and_exact_resources():
    command = controller.volume_create_args(args())
    assert command[:3] == ["volumes", "create", "gf_qual_vol"]
    assert command[command.index("--region") + 1] == "den"
    assert command[command.index("--size") + 1] == "10"
    assert "--scheduled-snapshots=false" in command


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


def test_refuses_missing_evidence_output_parent(tmp_path):
    with pytest.raises(controller.QualificationError, match="parent directory"):
        controller.validate_inputs(args(evidence_out=tmp_path / "missing" / "evidence.json"))


def test_refuses_dirty_or_non_exact_source(monkeypatch):
    responses = iter(
        [
            argparse.Namespace(stdout="b" * 40 + "\n"),
            argparse.Namespace(stdout=""),
        ]
    )
    monkeypatch.setattr(controller.subprocess, "run", lambda *_args, **_kwargs: next(responses))
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
    payload = controller.machine_create_payload(args(), "internal-volume-id", "sha256:" + "b" * 64)
    assert payload["region"] == "den"
    assert payload["skip_launch"] is False
    assert payload["skip_service_registration"] is True
    config = payload["config"]
    assert config["image"].endswith("@sha256:" + "b" * 64)
    assert config["auto_destroy"] is True
    assert config["restart"] == {"policy": "no"}
    assert config["guest"] == {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096}
    assert config["mounts"] == [{"volume": "internal-volume-id", "path": "/work"}]
    assert config["services"] == []


def test_machine_api_uses_memory_only_token_and_sanitizes_http_failure(monkeypatch):
    class TokenFly:
        def run(self, command, check=True):
            assert command == ["auth", "token"]
            return argparse.Namespace(stdout="super-secret-token\n")

    def reject(request, timeout):
        assert timeout == 120
        assert request.full_url.endswith("/apps/gf-qual-app/machines")
        assert request.headers["Authorization"] == "Bearer super-secret-token"
        raise controller.urllib.error.HTTPError(
            request.full_url, 422, "response body must not leak", {}, None
        )

    monkeypatch.setattr(controller.urllib.request, "urlopen", reject)
    with pytest.raises(controller.QualificationError, match="HTTP 422") as failure:
        controller.create_machine(args(), TokenFly(), "internal-volume-id", "sha256:" + "b" * 64)
    assert "super-secret-token" not in str(failure.value)
    assert "response body" not in str(failure.value)


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
    machine["config"]["mounts"] = [{"path": "/not-work"}]
    with pytest.raises(controller.QualificationError, match="mounted"):
        controller.assert_machine_config(machine, args(), "sha256:" + "b" * 64)
    machine["config"]["mounts"] = [{"path": "/work"}]
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
    for windows_path in (r"\\server\share", r"\rooted\path"):
        with pytest.raises(validator.EvidenceError, match="absolute path"):
            validator.reject_sensitive(windows_path)


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


def test_teardown_continues_after_each_timeout():
    class TimingOutFly(FakeFly):
        def run(self, command, check=True):
            self.calls.append(command)
            raise subprocess.TimeoutExpired(command, 120)

    fake = TimingOutFly()
    controller.cleanup(fake, "gf-qual-app", "machine-internal", "volume-internal")
    assert [call[:2] for call in fake.calls] == [
        ["machine", "destroy"],
        ["volumes", "destroy"],
        ["apps", "destroy"],
    ]


def test_schema_is_closed_and_committed():
    schema = json.loads(
        (ROOT / "docs/development/evidence/fly-filesystem-qualification.schema.json").read_text()
    )
    assert schema["additionalProperties"] is False
    assert schema["properties"]["host"]["additionalProperties"] is False
