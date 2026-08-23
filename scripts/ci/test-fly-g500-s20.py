#!/usr/bin/env python3
"""Deterministic contract tests for the disposable Fly S20 controller."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[2]
CONTROLLER = ROOT / "scripts/fly-g500-s20.py"
spec = importlib.util.spec_from_file_location("fly_g500_s20", CONTROLLER)
assert spec and spec.loader
controller = importlib.util.module_from_spec(spec)
spec.loader.exec_module(controller)


def pricing_html(hour: str = "0.1076") -> str:
    second = f"{float(hour) / 3600:.8f}"
    return f"""
      <div id="started-machines-pricing-matrix-dfw"><table><tr>
      <th>performance-2x</th><td>2 performance</td><td>4GB</td>
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
        evidence_out=root / "evidence.json",
        journal_out=root / "journal.json",
        execute=False,
        confirm_disposable=False,
    )


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        parsed = controller.parse_live_rates(pricing_html(), "dfw")
        assert parsed == {"compute_per_hour_usd": 0.1076, "volume_gb_month_usd": 0.15}
        cost = controller.cost_plan(parsed, 10.0, 1.0)
        assert cost["projected_max_usd"] < 10.0
        try:
            controller.cost_plan(
                {"compute_per_hour_usd": 3.0, "volume_gb_month_usd": 1.0}, 10.0, 1.0
            )
        except controller.ControllerError:
            pass
        else:
            raise AssertionError("over-budget live rates must be refused")

        child = json.dumps(
            {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json"}
        )
        controller.assert_platform_child("unused", child)
        try:
            controller.assert_platform_child(
                "unused",
                json.dumps(
                    {"mediaType": "application/vnd.oci.image.index.v1+json", "manifests": []}
                ),
            )
        except controller.ControllerError:
            pass
        else:
            raise AssertionError("OCI index must be refused")

        options = args(root)
        digest = controller.validate_args(options)
        assert digest == "sha256:" + "b" * 64
        payload = controller.machine_payload(options, "vol_test")
        config = payload["config"]
        assert config["services"] == []
        assert config["restart"] == {"policy": "no"}
        assert config["auto_destroy"] is True
        assert config["guest"] == {"cpu_kind": "performance", "cpus": 2, "memory_mb": 4096}
        assert config["mounts"] == [{"volume": "vol_test", "path": "/work"}]

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
            "git_sha": "a" * 40,
            "result": "pass",
            "lifecycle": lifecycle,
        }
        controller.validate_evidence(evidence, phases, "a" * 40)
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

        (root / "pricing.html").write_text(pricing_html())
        (root / "manifest.json").write_text(child)
        # Main dry-run exercises argument/config/rate/manifest validation without Fly.
        import subprocess

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
                "--evidence-out",
                str(options.evidence_out),
                "--journal-out",
                str(options.journal_out),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        plan = json.loads(result.stdout)
        assert plan["mode"] == "dry-run" and plan["hard_ttl_s"] == 16200
        assert plan["volume_gb"] == 50 and plan["public_services"] == 0
        assert plan["heartbeat_interval_s"] == 60
        assert plan["phase_timeout_s"]["ingest"] == 3600
        assert plan["phase_timeout_s"]["source_query_2hop"] == 900

    source = CONTROLLER.read_text()
    assert "Authorization" in source and '["auth", "token"]' in source
    assert "finally:" in source and "destroy_and_verify" in source
    assert "FLY_API_TOKEN" not in source
    print("Fly S20 controller tests passed")


if __name__ == "__main__":
    main()
