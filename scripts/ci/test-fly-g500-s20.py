#!/usr/bin/env python3
"""Deterministic contract tests for the disposable Fly S20 controller."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CONTROLLER = ROOT / "scripts/fly-g500-s20.py"
spec = importlib.util.spec_from_file_location("fly_g500_s20", CONTROLLER)
assert spec and spec.loader
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


def pricing_html(
    hour: str = "0.1076",
    machine: str = "performance-2x",
    cpus: int = 2,
    memory_gb: int = 4,
) -> str:
    second = f"{float(hour) / 3600:.8f}"
    return f"""
      <div id="started-machines-pricing-matrix-dfw"><table><tr>
      <th>{machine}</th><td>{cpus} performance</td><td>{memory_gb}GB</td>
      <td>${second}</td><td>${hour}</td></tr></table></div>
      <p>$0.15/GB per month of provisioned capacity</p>
    """


def args(root: Path) -> argparse.Namespace:
    return argparse.Namespace(
        expected_sha="a" * 40,
        image="registry.fly.io/gf-s20@sha256:" + "b" * 64,
        region="dfw",
        org="personal",
        app_name="gf-s20-test",
        machine_name="gf-s20-machine",
        volume_name="gf_s20_volume",
        ceiling_usd=10.0,
        unpriced_reserve_usd=1.0,
        pricing_html=root / "pricing.html",
        manifest_json=root / "manifest.json",
        image_contract_json=root / "image-contract.json",
        qualification_evidence=root / "qualification.json",
        evidence_out=root / "evidence.json",
        journal_out=root / "journal.json",
        diagnostic_out=root / "diagnostic.json",
        execute=False,
        confirm_disposable=False,
    )


def qualification(digest: str, *, growth: float = 1.1, disk_gib: int = 20) -> dict:
    def phase(identifier: str, peak: int) -> dict:
        return {
            "id": identifier,
            "elapsed_ms": 1,
            "memory": {
                "cgroup_current_before_bytes": peak // 2,
                "cgroup_peak_bytes": peak,
                "cgroup_current_after_bytes": peak // 2,
                "smaps_rss_bytes": peak // 2,
                "smaps_anon_bytes": peak // 3,
                "smaps_file_bytes": peak // 6,
            },
            "io": {
                "read_bytes": 1_048_576,
                "write_bytes": 1_048_576,
                "read_syscalls": 1,
                "write_syscalls": 1,
                "blocks": 1,
                "batches": 1,
                "shards": 1,
            },
        }

    base = 2 * 1024**3
    return {
        "schema": "graphforge-fly-s20-qualification/1",
        "region": "dfw",
        "image_digest": digest,
        "max_phase_rss_growth_ratio": 1.2,
        "machine_candidates": [
            {"name": "performance-2x", "cpus": 2, "memory_mb": 4096},
            {"name": "performance-4x", "cpus": 4, "memory_mb": 8192},
        ],
        "rungs": [
            {
                "scale": 18,
                "result": "pass",
                "physical_volume_peak_bytes": 1024**3,
                "s20_projected_physical_peak_bytes": disk_gib * 1024**3,
                "budgets": {"memory_bytes": 3 * 1024**3, "batch_rows": 65_536},
                "runtime": {"machine": "performance-2x", "cpus": 2, "memory_mb": 4096},
                "construction": {
                    "contract": controller.REQUIRED_CONSTRUCTION_CONTRACT,
                    "source_current_transitions": 1,
                    "import_current_transitions": 1,
                },
                "phases": [phase(identifier, base) for identifier in controller.PHASES],
            },
            {
                "scale": 19,
                "result": "pass",
                "physical_volume_peak_bytes": 2 * 1024**3,
                "s20_projected_physical_peak_bytes": disk_gib * 1024**3,
                "budgets": {"memory_bytes": 3 * 1024**3, "batch_rows": 65_536},
                "runtime": {"machine": "performance-2x", "cpus": 2, "memory_mb": 4096},
                "construction": {
                    "contract": controller.REQUIRED_CONSTRUCTION_CONTRACT,
                    "source_current_transitions": 1,
                    "import_current_transitions": 1,
                },
                "phases": [
                    phase(identifier, int(base * growth)) for identifier in controller.PHASES
                ],
            },
        ],
    }


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        parsed = controller.parse_live_rates(pricing_html(), "dfw", "performance-2x", 2, 4096)
        assert parsed == {"compute_per_hour_usd": 0.1076, "volume_gb_month_usd": 0.15}
        cost = controller.cost_plan(parsed, 10.0, 1.0, 25)
        assert cost["projected_max_usd"] < 10.0
        try:
            controller.cost_plan(
                {"compute_per_hour_usd": 3.0, "volume_gb_month_usd": 1.0}, 10.0, 1.0, 25
            )
        except controller.ControllerError:
            pass
        else:
            raise AssertionError("over-budget live rates must be refused")

        child = json.dumps(
            {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}
        )
        image_contract = json.dumps(
            {
                "architecture": "amd64",
                "os": "linux",
                "config": {
                    "Labels": {
                        "org.opencontainers.image.revision": "a" * 40,
                        "dev.graphforge.s20.runtime": controller.REQUIRED_IMAGE_CONTRACT,
                        "dev.graphforge.s20.measurement": controller.REQUIRED_MEASUREMENT_CONTRACT,
                        "dev.graphforge.s20.construction": controller.REQUIRED_CONSTRUCTION_CONTRACT,
                    }
                },
            }
        )
        controller.assert_platform_child("unused", "a" * 40, child, image_contract)
        wrong_platform = json.loads(image_contract)
        wrong_platform["architecture"] = "arm64"
        try:
            controller.assert_platform_child(
                "unused", "a" * 40, child, json.dumps(wrong_platform)
            )
        except controller.ControllerError as error:
            assert "linux/amd64" in str(error)
        else:
            raise AssertionError("wrong-platform child must be refused")
        try:
            controller.assert_platform_child(
                "unused",
                "a" * 40,
                json.dumps(
                    {"mediaType": "application/vnd.oci.image.index.v1+json", "manifests": []}
                ),
                image_contract,
            )
        except controller.ControllerError:
            pass
        else:
            raise AssertionError("OCI index must be refused")

        options = args(root)
        digest = controller.validate_args(options)
        assert digest == "sha256:" + "b" * 64
        options.qualification_evidence.write_text(json.dumps(qualification(digest)))

        options.qualification_evidence.write_text(json.dumps(qualification(digest, disk_gib=500)))
        try:
            controller.load_qualification(options.qualification_evidence, digest, "dfw")
        except controller.ControllerError as error:
            assert "500 GB" in str(error)
        else:
            raise AssertionError("Fly volume overflow must be refused before execution")
        options.qualification_evidence.write_text(json.dumps(qualification(digest)))
        resources = controller.load_qualification(options.qualification_evidence, digest, "dfw")
        assert resources["machine"] == "performance-2x"
        assert resources["memory_mb"] == 4096
        assert resources["volume_gb"] == 25
        inconsistent = qualification(digest)
        inconsistent["rungs"][1]["budgets"]["batch_rows"] = 32_768
        options.qualification_evidence.write_text(json.dumps(inconsistent))
        try:
            controller.load_qualification(options.qualification_evidence, digest, "dfw")
        except controller.ControllerError as error:
            assert "different operator budgets" in str(error)
        else:
            raise AssertionError("different lower-rung budgets must be refused")
        options.qualification_evidence.write_text(json.dumps(qualification(digest)))
        payload = controller.machine_payload(options, "vol_test", resources)
        config = payload["config"]
        assert config["services"] == []
        assert config["restart"] == {"policy": "no"}
        assert config["auto_destroy"] is True
        assert config["guest"] == {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096}
        assert config["mounts"] == [{"volume": "vol_test", "path": "/work"}]

        options.qualification_evidence.write_text(json.dumps(qualification(digest, growth=1.3)))
        try:
            controller.load_qualification(options.qualification_evidence, digest, "dfw")
        except controller.ControllerError as error:
            assert "does not plateau" in str(error)
        else:
            raise AssertionError("material phase RSS growth must be refused")
        options.qualification_evidence.write_text(json.dumps(qualification(digest)))

        phases = [{"id": phase, "status": "pass"} for phase in controller.PHASES]
        lifecycle = {
            "phases": phases,
            "source_edges": 1,
            "imported_edges": 1,
            "source_project_fingerprint": "sha256:x",
            "imported_project_fingerprint": "sha256:x",
            "source_authority_fingerprint": "sha256:y",
            "imported_authority_fingerprint": "sha256:y",
        }
        evidence = {
            "schema": "graphforge-s20-integrated-lifecycle-evidence/1",
            "measurement_contract": controller.REQUIRED_MEASUREMENT_CONTRACT,
            "construction_contract": controller.REQUIRED_CONSTRUCTION_CONTRACT,
            "git_sha": "a" * 40,
            "result": "pass",
            "lifecycle": lifecycle,
        }
        controller.validate_evidence(evidence, phases, "a" * 40)
        measured_phases = []
        for phase in phases:
            measured_phases.append(
                {
                    **phase,
                    "memory": {
                        "cgroup_current_before_bytes": 1,
                        "cgroup_peak_bytes": 2,
                        "cgroup_current_after_bytes": 1,
                        "smaps_rss_before_bytes": 1,
                        "smaps_rss_after_bytes": 1,
                        "smaps_anon_before_bytes": 1,
                        "smaps_anon_after_bytes": 1,
                        "smaps_file_before_bytes": 0,
                        "smaps_file_after_bytes": 0,
                    },
                    "io": {
                        "proc_read_bytes": 1,
                        "proc_write_bytes": 1,
                        "proc_read_syscalls": 1,
                        "proc_write_syscalls": 1,
                        "storage_sequential_bytes": 1,
                        "storage_blocks": 1,
                        "arrow_batches": 1,
                        "max_arrow_batch_rows": 1,
                        "shards": 1,
                        "row_groups": 1,
                        "random_seeks": 0,
                        "fsyncs": 1,
                        "topology_rows": 100,
                    },
                    "filesystem": {
                        "total_bytes": resources["volume_gb"] * 1024**3,
                        "free_before_bytes": resources["volume_gb"] * 1024**3 - 10,
                        "free_after_bytes": resources["volume_gb"] * 1024**3 - 11,
                        "available_before_bytes": resources["volume_gb"] * 1024**3 - 10,
                        "available_after_bytes": resources["volume_gb"] * 1024**3 - 11,
                        "allocated_before_bytes": 10,
                        "allocated_after_bytes": 11,
                    },
                    "memory_limit_bytes": resources["memory_mb"] * 1024**2,
                }
            )
        measured = json.loads(json.dumps(evidence))
        measured["lifecycle"]["phases"] = measured_phases
        measured["lifecycle"]["current_transitions"] = {"source": 1, "clean_import": 1}
        measured["run_environment"] = {
            "region": "dfw",
            "image_digest": digest,
            "machine": resources["machine"],
            "cpus": resources["cpus"],
            "memory_mb": resources["memory_mb"],
            "volume_gb": resources["volume_gb"],
            "public_services": 0,
            "restart": "no",
        }
        measured["resource_gates"] = {"rss_plateau": "pass", "disk_headroom": "pass"}
        bound_resources = {**resources, "region": "dfw", "image_digest": digest}
        controller.validate_evidence(measured, measured_phases, "a" * 40, bound_resources)
        measured["lifecycle"]["phases"][0]["io"].pop("storage_blocks")
        try:
            controller.validate_evidence(measured, measured_phases, "a" * 40, bound_resources)
        except controller.ControllerError as error:
            assert "storage I/O evidence" in str(error)
        else:
            raise AssertionError("missing block I/O evidence must be refused")
        evidence["lifecycle"]["imported_edges"] = 2
        try:
            controller.validate_evidence(evidence, phases, "a" * 40)
        except controller.ControllerError:
            pass
        else:
            raise AssertionError("non-equivalent import must be refused")

        assert controller.journal_progress(phases[:3]) == (3, "csr")
        failed = [*phases[:2], {"id": "ingest", "status": "fail", "failure_code": "oom"}]
        try:
            controller.journal_progress(failed)
        except controller.ControllerError as error:
            assert str(error) == "phase_failed phase=ingest failure_code=oom"
        else:
            raise AssertionError("typed phase failure must stop the controller")
        try:
            controller.journal_progress([{"id": "generate", "status": "pass"}])
        except controller.ControllerError as error:
            assert str(error).startswith("journal_invalid")
        else:
            raise AssertionError("out-of-order journal must be refused")

        unsafe = {
            "schema": "incomplete",
            "token": "must-not-survive",
            "nested": {"password_hint": "must-not-survive", "message": "bounded"},
        }
        controller.write_sanitized_json(options.evidence_out, unsafe)
        preserved = json.loads(options.evidence_out.read_text())
        assert preserved["schema"] == "incomplete"
        assert preserved["token"] == "<redacted>"
        assert preserved["nested"]["password_hint"] == "<redacted>"
        authority = "sha256:not-a-credential"
        controller.write_sanitized_json(
            options.evidence_out, {"source_authority_fingerprint": authority}
        )
        assert json.loads(options.evidence_out.read_text())["source_authority_fingerprint"] == authority
        try:
            controller.preserve_and_validate_evidence(
                unsafe,
                phases[:2],
                options.expected_sha,
                options.evidence_out,
                options.journal_out,
            )
        except controller.ControllerError:
            pass
        else:
            raise AssertionError("incomplete evidence must not validate")
        assert json.loads(options.evidence_out.read_text())["schema"] == "incomplete"
        assert len(json.loads(options.journal_out.read_text())) == 2

        class FakeFly:
            def run(self, _arguments, *, check=True):
                del check
                return subprocess.CompletedProcess(
                    [],
                    0,
                    json.dumps(
                        {
                            "state": "stopped",
                            "region": "dfw",
                            "private_ip": "must-not-survive",
                            "events": [
                                {
                                    "type": "exit",
                                    "status": "failed",
                                    "exit_code": 137,
                                    "request": "must-not-survive",
                                }
                            ],
                        }
                    ),
                    "",
                )

        diagnostic = controller.machine_diagnostic(FakeFly(), options.app_name, "machine-id")
        assert diagnostic == {
            "available": True,
            "state": "stopped",
            "region": "dfw",
            "events": [{"type": "exit", "status": "failed", "exit_code": 137}],
        }

        class FakeCleanup:
            def __init__(self):
                self.calls = []

            def run(self, arguments, *, check=True, timeout=120):
                del check
                del timeout
                self.calls.append(arguments)
                return subprocess.CompletedProcess([], 0, "", "")

            def json(self, arguments, *, timeout=120):
                del timeout
                self.calls.append(arguments)
                return []

        cleanup = FakeCleanup()
        controller.destroy_and_verify(cleanup, "gf-s20-test", "machine-id", "volume-id")
        assert [
            "machine",
            "destroy",
            "machine-id",
            "--app",
            "gf-s20-test",
            "--force",
        ] in cleanup.calls
        assert ["volumes", "destroy", "volume-id", "--app", "gf-s20-test", "--yes"] in cleanup.calls
        assert ["apps", "destroy", "gf-s20-test", "--yes"] in cleanup.calls

        class FirstDeleteFails(FakeCleanup):
            def run(self, arguments, *, check=True, timeout=120):
                if not self.calls:
                    self.calls.append(arguments)
                    raise subprocess.TimeoutExpired(arguments, timeout)
                return super().run(arguments, check=check, timeout=timeout)

        cleanup_after_failure = FirstDeleteFails()
        controller.destroy_and_verify(
            cleanup_after_failure, "gf-s20-test", "machine-id", "volume-id"
        )
        assert [
            "volumes",
            "destroy",
            "volume-id",
            "--app",
            "gf-s20-test",
            "--yes",
        ] in cleanup_after_failure.calls
        assert ["apps", "destroy", "gf-s20-test", "--yes"] in cleanup_after_failure.calls

        (root / "pricing.html").write_text(pricing_html())
        (root / "manifest.json").write_text(child)
        (root / "image-contract.json").write_text(image_contract)
        # Main dry-run exercises argument/config/rate/manifest validation without Fly.
        result = subprocess.run(
            [
                "python3",
                str(CONTROLLER),
                "--expected-sha",
                "a" * 40,
                "--image",
                options.image,
                "--org",
                "personal",
                "--app-name",
                options.app_name,
                "--machine-name",
                options.machine_name,
                "--volume-name",
                options.volume_name,
                "--pricing-html",
                str(options.pricing_html),
                "--manifest-json",
                str(options.manifest_json),
                "--image-contract-json",
                str(options.image_contract_json),
                "--qualification-evidence",
                str(options.qualification_evidence),
                "--evidence-out",
                str(options.evidence_out),
                "--journal-out",
                str(options.journal_out),
                "--diagnostic-out",
                str(options.diagnostic_out),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        plan = json.loads(result.stdout)
        assert plan["mode"] == "dry-run" and plan["hard_ttl_s"] == 14400
        assert plan["volume_gb"] == 25 and plan["public_services"] == 0
        assert plan["qualification"]["qualified_peak_rss_bytes"] > 0
        assert plan["heartbeat_interval_s"] == 60
        assert plan["phase_timeout_s"]["ingest"] == 5400
        assert plan["phase_timeout_s"]["import"] == 5400
        assert plan["phase_timeout_s"]["source_query_2hop"] == 900

    source = CONTROLLER.read_text()
    assert "Authorization" in source and '["auth", "token"]' in source
    assert "finally:" in source and "destroy_and_verify" in source
    assert "FLY_API_TOKEN" not in source
    assert "du -" not in source
    print("Fly S20 controller tests passed")


if __name__ == "__main__":
    main()
